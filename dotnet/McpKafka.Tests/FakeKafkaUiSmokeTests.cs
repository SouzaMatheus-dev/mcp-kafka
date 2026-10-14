using System.Net;
using System.Net.Sockets;
using System.Text;
using McpKafka.Services;

namespace McpKafka.Tests;

public class FakeKafkaUiSmokeTests
{
    private const string Topico = "pedidos.integracao.retorno";

    [Fact]
    public async Task PortalSimuladoEntregaMensagemPorProcessoId()
    {
        await using var portal = FakeKafkaUi.Start();
        var config = new KafkaConfig(portal.BaseUrl + "/auth", "admin", "secret", null, false, 20, 15);
        using var client = new KafkaUiClient(config);

        var texto = await client.BuscarMensagensAsync(Topico, "PROC-9", null, 5, "recentes", null);

        Assert.Contains("PROC-9", texto, StringComparison.Ordinal);
        Assert.Contains("partition=1", texto, StringComparison.Ordinal);
        Assert.Contains("formulario POST /auth", client.AuthMode, StringComparison.Ordinal);
        Assert.Contains("q=PROC-9", portal.LastMessagesQuery, StringComparison.Ordinal);
        Assert.Contains("seekType=LATEST", portal.LastMessagesQuery, StringComparison.Ordinal);
        Assert.True(portal.MessagesHadSession);
    }

    private sealed class FakeKafkaUi : IAsyncDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _loop;

        private FakeKafkaUi(int port)
        {
            BaseUrl = "http://127.0.0.1:" + port;
            _listener.Prefixes.Add(BaseUrl + "/");
            _listener.Start();
            _loop = Task.Run(LoopAsync);
        }

        public string BaseUrl { get; }

        public string? LastMessagesQuery { get; private set; }

        public bool MessagesHadSession { get; private set; }

        public static FakeKafkaUi Start()
        {
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return new FakeKafkaUi(port);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _listener.Stop();
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        private async Task LoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().WaitAsync(_cts.Token).ConfigureAwait(false);
                }
                catch (Exception) when (_cts.IsCancellationRequested)
                {
                    break;
                }

                await HandleAsync(context).ConfigureAwait(false);
            }
        }

        private async Task HandleAsync(HttpListenerContext context)
        {
            var path = context.Request.Url?.AbsolutePath ?? "";
            var query = context.Request.Url?.Query ?? "";
            var cookie = context.Request.Headers["Cookie"] ?? "";

            if (context.Request.HttpMethod == "POST" && path == "/auth")
            {
                using var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8);
                var body = await reader.ReadToEndAsync().ConfigureAwait(false);
                if (!body.Contains("username=admin", StringComparison.Ordinal) ||
                    !body.Contains("password=secret", StringComparison.Ordinal))
                {
                    context.Response.StatusCode = 401;
                    context.Response.Close();
                    return;
                }

                context.Response.StatusCode = 302;
                context.Response.RedirectLocation = "/";
                context.Response.Headers.Add("Set-Cookie", "SESSION=smoke; Path=/; HttpOnly");
                context.Response.Close();
                return;
            }

            if (!cookie.Contains("SESSION=smoke", StringComparison.Ordinal))
            {
                context.Response.StatusCode = 401;
                context.Response.Close();
                return;
            }

            if (path == "/api/clusters")
            {
                await WriteAsync(context, 200, "application/json",
                    "[{\"name\":\"local\",\"defaultCluster\":true,\"status\":\"online\",\"brokerCount\":1,\"topicCount\":1}]")
                    .ConfigureAwait(false);
                return;
            }

            if (path.Contains("/messages", StringComparison.Ordinal))
            {
                LastMessagesQuery = query;
                MessagesHadSession = true;
                var sse =
                    "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":1,\"offset\":4,\"timestamp\":\"2026-10-14T18:00:00Z\",\"content\":\"{\\\"ProcessoID\\\":\\\"PROC-9\\\"}\"}}\n\n" +
                    "data: {\"type\":\"DONE\"}\n\n";
                await WriteAsync(context, 200, "text/event-stream", sse).ConfigureAwait(false);
                return;
            }

            context.Response.StatusCode = 404;
            context.Response.Close();
        }

        private static async Task WriteAsync(HttpListenerContext context, int status, string media, string body)
        {
            var bytes = Encoding.UTF8.GetBytes(body);
            context.Response.StatusCode = status;
            context.Response.ContentType = media;
            context.Response.ContentLength64 = bytes.Length;
            await context.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
            context.Response.Close();
        }
    }
}

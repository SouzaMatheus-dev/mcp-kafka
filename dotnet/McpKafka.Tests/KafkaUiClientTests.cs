using System.Net;
using System.Text;
using McpKafka.Services;
using McpKafka.Tools;

namespace McpKafka.Tests;

public class KafkaUiClientTests
{
    private const string Topico = "pedidos.integracao.envio";

    [Fact]
    public async Task BuscaProcessoIdComCookieDeLogin()
    {
        var handler = new RecordingHandler();
        using var client = Client(handler);

        var texto = await client.BuscarMensagensAsync(Topico, "PROC-42", null, 10, "recentes", null);

        Assert.Contains("PROC-42", texto, StringComparison.Ordinal);
        Assert.Contains("partition=2", texto, StringComparison.Ordinal);
        Assert.Contains("seekType=LATEST", texto, StringComparison.Ordinal);
        Assert.Contains("seekDirection=BACKWARD", texto, StringComparison.Ordinal);
        Assert.Equal("formulario POST /auth", client.AuthMode);
        Assert.Contains("username=admin", handler.LoginBody, StringComparison.Ordinal);
        Assert.DoesNotContain("segredo-teste", texto, StringComparison.Ordinal);

        var messages = handler.Requests.Single(r => r.Path.Contains("/messages", StringComparison.Ordinal));
        Assert.Contains("q=PROC-42", messages.Path, StringComparison.Ordinal);
        Assert.Contains("filterQueryType=STRING_CONTAINS", messages.Path, StringComparison.Ordinal);
        Assert.Contains("SESSION=abc", messages.Cookie, StringComparison.Ordinal);
        Assert.Null(messages.Authorization);
    }

    [Fact]
    public async Task OrigemInicioUsaBeginningForward()
    {
        var handler = new RecordingHandler();
        using var client = Client(handler);

        var texto = await client.BuscarMensagensAsync(Topico, "PROC-42", null, 5, "inicio", null);

        Assert.Contains("origem=inicio", texto, StringComparison.Ordinal);
        var messages = handler.Requests.Single(r => r.Path.Contains("/messages", StringComparison.Ordinal));
        Assert.Contains("seekType=BEGINNING", messages.Path, StringComparison.Ordinal);
        Assert.Contains("seekDirection=FORWARD", messages.Path, StringComparison.Ordinal);
    }

    [Fact]
    public async Task VariosClustersSemDefaultPedeEscolha()
    {
        var handler = new RecordingHandler { ClustersJson = """
            [
              {"name":"a","defaultCluster":false,"status":"online","brokerCount":1,"topicCount":2},
              {"name":"b","defaultCluster":false,"status":"online","brokerCount":1,"topicCount":3}
            ]
            """ };
        using var client = Client(handler);
        var tools = new KafkaTools(Config(), client);

        var texto = await tools.ListarTopicos("pedidos");

        Assert.Contains("Há vários clusters (a, b)", texto, StringComparison.Ordinal);
        Assert.DoesNotContain(handler.Requests, r => r.Path.Contains("/topics", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SemUrlBloqueiaAntesDaRede()
    {
        var handler = new RecordingHandler();
        var config = new KafkaConfig(null, "admin", "segredo-teste", null, false, 20, 15);
        using var client = new KafkaUiClient(config, handler);
        var tools = new KafkaTools(config, client);

        var texto = await tools.ContextoAtual();

        Assert.StartsWith("Bloqueado:", texto, StringComparison.Ordinal);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task ModoAoVivoFicaDeFora()
    {
        var handler = new RecordingHandler();
        using var client = Client(handler);

        var texto = await client.BuscarMensagensAsync(Topico, "PROC-42", null, 5, "tailing", null);

        Assert.StartsWith("Bloqueado:", texto, StringComparison.Ordinal);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task CaiParaBasicQuandoOFormularioNaoExiste()
    {
        var handler = new RecordingHandler { FormLoginStatus = HttpStatusCode.NotFound };
        using var client = Client(handler);

        var texto = await client.ListarClustersAsync();

        Assert.Contains("local", texto, StringComparison.Ordinal);
        Assert.Equal("basic", client.AuthMode);
        var clusters = handler.Requests.Single(r => r.Path.EndsWith("/api/clusters", StringComparison.Ordinal));
        Assert.StartsWith("Basic ", clusters.Authorization, StringComparison.Ordinal);
    }

    private static KafkaConfig Config() =>
        new("https://kafka.exemplo/auth", "admin", "segredo-teste", null, false, 20, 15);

    private static KafkaUiClient Client(RecordingHandler handler) => new(Config(), handler);

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public List<SeenRequest> Requests { get; } = [];

        public string LoginBody { get; private set; } = "";

        public string ClustersJson { get; init; } =
            """
            [{"name":"local","defaultCluster":true,"status":"online","brokerCount":3,"topicCount":4}]
            """;

        public HttpStatusCode FormLoginStatus { get; init; } = HttpStatusCode.Found;

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var path = request.RequestUri?.PathAndQuery ?? "";
            var cookie = request.Headers.TryGetValues("Cookie", out var cookies)
                ? string.Join("; ", cookies)
                : null;
            var authorization = request.Headers.Authorization?.ToString();
            Requests.Add(new SeenRequest(path, cookie, authorization));

            if (request.Method == HttpMethod.Post &&
                (path == "/auth" || path == "/login"))
            {
                if (request.Content is not null)
                {
                    LoginBody = await request.Content.ReadAsStringAsync(cancellationToken);
                }

                var response = new HttpResponseMessage(FormLoginStatus);
                if (FormLoginStatus == HttpStatusCode.Found)
                {
                    response.Headers.Location = new Uri("https://kafka.exemplo/");
                    response.Headers.TryAddWithoutValidation("Set-Cookie", "SESSION=abc; Path=/; HttpOnly");
                }

                return response;
            }

            if (FormLoginStatus == HttpStatusCode.Found &&
                (cookie is null || !cookie.Contains("SESSION=abc", StringComparison.Ordinal)))
            {
                return new HttpResponseMessage(HttpStatusCode.Unauthorized);
            }

            if (FormLoginStatus == HttpStatusCode.NotFound &&
                (authorization is null || !authorization.StartsWith("Basic ", StringComparison.Ordinal)))
            {
                return new HttpResponseMessage(HttpStatusCode.Unauthorized);
            }

            if (path.StartsWith("/api/clusters/local/topics/" + Topico + "/messages", StringComparison.Ordinal))
            {
                return JsonOrSse(Sse(), "text/event-stream");
            }

            if (path.StartsWith("/api/clusters/local/topics/" + Topico, StringComparison.Ordinal))
            {
                return Json(
                    "{\"name\":\"" + Topico + "\",\"internal\":false,\"partitionCount\":6,\"replicationFactor\":3,\"inSyncReplicas\":3,\"underReplicatedPartitions\":0,\"segmentSize\":10,\"cleanUpPolicy\":\"DELETE\"}");
            }

            if (path.StartsWith("/api/clusters/local/topics", StringComparison.Ordinal))
            {
                return Json(
                    "{\"pageCount\":1,\"topics\":[{\"name\":\"" + Topico + "\",\"internal\":false,\"partitionCount\":6,\"replicationFactor\":3,\"underReplicatedPartitions\":0,\"cleanUpPolicy\":\"DELETE\"}]}");
            }

            if (path.StartsWith("/api/clusters", StringComparison.Ordinal))
            {
                return Json(ClustersJson);
            }

            return new HttpResponseMessage(HttpStatusCode.NotFound);
        }

        private static HttpResponseMessage Json(string body) => JsonOrSse(body, "application/json");

        private static HttpResponseMessage JsonOrSse(string body, string media)
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(body, Encoding.UTF8, media),
            };
            return response;
        }

        private static string Sse() =>
            "data: {\"type\":\"CONSUMING\",\"consuming\":{\"messagesConsumed\":8,\"elapsedMs\":12,\"bytesConsumed\":40,\"isCancelled\":false,\"filterApplyErrors\":0}}\n\n" +
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":2,\"offset\":99,\"timestamp\":\"2026-10-14T12:00:00Z\",\"key\":\"proc\",\"content\":\"{\\\"ProcessoID\\\":\\\"PROC-42\\\",\\\"etapa\\\":\\\"envio\\\"}\",\"valueSerde\":\"String\"}}\n\n" +
            "data: {\"type\":\"DONE\"}\n\n";
    }

    internal sealed record SeenRequest(string Path, string? Cookie, string? Authorization);
}

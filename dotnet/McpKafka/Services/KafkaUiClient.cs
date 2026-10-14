using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace McpKafka.Services;

public sealed class KafkaUiException : Exception
{
    public KafkaUiException(string message) : base(message)
    {
    }
}

public sealed class KafkaUiClient : IDisposable
{
    private readonly KafkaConfig _config;
    private readonly HttpClient _http;
    private readonly SemaphoreSlim _loginLock = new(1, 1);
    private readonly Dictionary<string, string> _cookies = new(StringComparer.Ordinal);
    private bool _loginAttempted;
    private bool _formSession;
    private string? _bearer;
    private string _authMode = "pendente";

    public KafkaUiClient(KafkaConfig config)
        : this(config, CreateHandler(config))
    {
    }

    internal KafkaUiClient(KafkaConfig config, HttpMessageHandler handler)
    {
        _config = config;
        _http = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(config.TimeoutSeconds),
        };
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("McpKafka/0.1.0");
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public string AuthMode => _authMode;

    public void Dispose()
    {
        _http.Dispose();
        _loginLock.Dispose();
    }

    public async Task<string> DescreverContextoAsync(CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl();
        if (blocked is not null)
        {
            return blocked;
        }

        var lines = new List<string>
        {
            "modo=somente_leitura",
            "portal=" + _config.BaseUrl,
            "cluster_configurado=" + (_config.Cluster ?? "(descoberta automática)"),
            "tls_inseguro=" + (_config.SkipTlsVerify ? "true" : "false"),
            "usuario=" + (_config.User ?? "(não definido)"),
        };

        try
        {
            var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
            lines.Add("auth_modo=" + _authMode);
            lines.Add("clusters=" + clusters.Count);
            foreach (var cluster in clusters)
            {
                lines.Add(FormatCluster(cluster));
            }
        }
        catch (KafkaUiException ex)
        {
            lines.Add("auth_modo=" + _authMode);
            lines.Add(ex.Message);
        }

        return string.Join(Environment.NewLine, lines);
    }

    public async Task<string> DiagnosticarAsync(CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl();
        if (blocked is not null)
        {
            return blocked;
        }

        var lines = new List<string>
        {
            "portal=" + _config.BaseUrl,
            "usuario=" + (_config.User ?? "(não definido)"),
        };

        try
        {
            var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
            lines.Add("login=" + _authMode);
            lines.Add("GET /api/clusters=ok total=" + clusters.Count);
            foreach (var cluster in clusters)
            {
                lines.Add(FormatCluster(cluster));
            }

            if (clusters.Count == 1 || clusters.Any(c => c.DefaultCluster) || _config.Cluster is not null)
            {
                var name = ResolveClusterName(null, clusters);
                var topics = await GetJsonAsync(
                    "api/clusters/" + Uri.EscapeDataString(name) + "/topics?page=1&perPage=1&showInternal=false",
                    cancellationToken).ConfigureAwait(false);
                var pageCount = topics.TryGetProperty("pageCount", out var pages) && pages.TryGetInt32(out var count)
                    ? count.ToString()
                    : "-";
                lines.Add("GET /api/clusters/" + name + "/topics=ok pageCount=" + pageCount);
            }
        }
        catch (KafkaUiException ex)
        {
            lines.Add("login=" + _authMode);
            lines.Add(ex.Message);
        }

        lines.Add("escrita=indisponível (sem produce, sem delete)");
        return string.Join(Environment.NewLine, lines);
    }

    public async Task<string> ListarClustersAsync(CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl();
        if (blocked is not null)
        {
            return blocked;
        }

        var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
        if (clusters.Count == 0)
        {
            return "Nenhum cluster visível no Kafka UI.";
        }

        var lines = new List<string> { "portal=" + _config.BaseUrl, "clusters=" + clusters.Count };
        lines.AddRange(clusters.Select(FormatCluster));
        return string.Join(Environment.NewLine, lines);
    }

    public async Task<string> ListarTopicosAsync(
        string? busca,
        string? cluster,
        bool incluirInternos,
        CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl();
        if (blocked is not null)
        {
            return blocked;
        }

        var filtro = busca?.Trim() ?? "";
        var invalid = KafkaRules.ValidateFilter(filtro, "busca");
        if (invalid is not null)
        {
            return invalid;
        }

        var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
        var name = ResolveClusterName(cluster, clusters);
        var perPage = Math.Min(_config.MaxMessages, 100);
        var path =
            "api/clusters/" + Uri.EscapeDataString(name) +
            "/topics?page=1&perPage=" + perPage +
            "&showInternal=" + (incluirInternos ? "true" : "false") +
            "&orderBy=NAME&sortOrder=ASC";
        if (filtro.Length > 0)
        {
            path += "&search=" + Uri.EscapeDataString(filtro);
        }

        var json = await GetJsonAsync(path, cancellationToken).ConfigureAwait(false);
        return FormatTopics(name, filtro, json);
    }

    public async Task<string> ObterTopicoAsync(
        string topico,
        string? cluster,
        CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl() ?? KafkaRules.ValidateTopic(topico);
        if (blocked is not null)
        {
            return blocked;
        }

        var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
        var name = ResolveClusterName(cluster, clusters);
        var path = "api/clusters/" + Uri.EscapeDataString(name) + "/topics/" + Uri.EscapeDataString(topico.Trim());
        var json = await GetJsonAsync(path, cancellationToken).ConfigureAwait(false);
        return FormatTopicDetail(name, json);
    }

    public async Task<string> BuscarMensagensAsync(
        string topico,
        string? processoId,
        string? texto,
        int limite,
        string? origem,
        string? cluster,
        CancellationToken cancellationToken = default)
    {
        var blocked = RequireUrl() ?? KafkaRules.ValidateTopic(topico);
        if (blocked is not null)
        {
            return blocked;
        }

        if (!KafkaRules.TryMapOrigem(origem, out var seekType, out var seekDirection, out var origemNome, out var origemErro))
        {
            return origemErro;
        }

        var filtro = !string.IsNullOrWhiteSpace(processoId) ? processoId.Trim() : texto?.Trim() ?? "";
        var invalid = KafkaRules.ValidateFilter(filtro, "filtro");
        if (invalid is not null)
        {
            return invalid;
        }

        if (limite < 1 || limite > _config.MaxMessages)
        {
            return "Bloqueado: limite deve estar entre 1 e " + _config.MaxMessages + ".";
        }

        var clusters = await LoadClustersAsync(cancellationToken).ConfigureAwait(false);
        var name = ResolveClusterName(cluster, clusters);
        var path = BuildMessagesPath(name, topico.Trim(), limite, seekType, seekDirection, filtro);
        var capture = await ReadMessagesAsync(path, limite, filtro, cancellationToken).ConfigureAwait(false);
        return MessageText.FormatBusca(name, topico.Trim(), filtro, origemNome, seekType, seekDirection, capture);
    }

    internal static string BuildMessagesPath(
        string cluster,
        string topic,
        int limit,
        string seekType,
        string seekDirection,
        string? filtro)
    {
        var path =
            "api/clusters/" + Uri.EscapeDataString(cluster) +
            "/topics/" + Uri.EscapeDataString(topic) +
            "/messages?limit=" + limit +
            "&seekType=" + seekType +
            "&seekDirection=" + seekDirection +
            "&filterQueryType=STRING_CONTAINS";
        if (!string.IsNullOrEmpty(filtro))
        {
            path += "&q=" + Uri.EscapeDataString(filtro);
        }

        return path;
    }

    private async Task<List<ClusterInfo>> LoadClustersAsync(CancellationToken cancellationToken)
    {
        var json = await GetJsonAsync("api/clusters", cancellationToken).ConfigureAwait(false);
        return ParseClusters(json);
    }

    private string ResolveClusterName(string? requested, IReadOnlyList<ClusterInfo> clusters)
    {
        if (!string.IsNullOrWhiteSpace(requested))
        {
            return requested.Trim();
        }

        if (!string.IsNullOrWhiteSpace(_config.Cluster))
        {
            return _config.Cluster;
        }

        if (clusters.Count == 0)
        {
            throw new KafkaUiException("Nenhum cluster visível no Kafka UI.");
        }

        var chosen = clusters.FirstOrDefault(c => c.DefaultCluster) ?? (clusters.Count == 1 ? clusters[0] : null);
        if (chosen is null)
        {
            var names = string.Join(", ", clusters.Select(c => c.Name));
            throw new KafkaUiException(
                "Há vários clusters (" + names + "). Informe o parâmetro cluster ou defina KAFKA_CLUSTER.");
        }

        return chosen.Name;
    }

    private async Task<MessageCapture> ReadMessagesAsync(
        string relative,
        int limit,
        string filtro,
        CancellationToken cancellationToken)
    {
        using var response = await SendApiAsync(HttpMethod.Get, relative, eventStream: true, cancellationToken)
            .ConfigureAwait(false);
        var bodyPreview = "";
        if (!response.IsSuccessStatusCode)
        {
            bodyPreview = await ReadPreviewAsync(response, cancellationToken).ConfigureAwait(false);
            throw new KafkaUiException(FormatHttpError(response.StatusCode, relative, bodyPreview));
        }

        var media = response.Content.Headers.ContentType?.MediaType ?? "";
        if (media.Contains("json", StringComparison.OrdinalIgnoreCase) &&
            !media.Contains("event-stream", StringComparison.OrdinalIgnoreCase))
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return SseParser.CollectBody(body, limit, string.IsNullOrEmpty(filtro) ? null : filtro);
        }

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await SseParser.CollectStreamAsync(
            stream,
            limit,
            string.IsNullOrEmpty(filtro) ? null : filtro,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<JsonElement> GetJsonAsync(string relative, CancellationToken cancellationToken)
    {
        using var response = await SendApiAsync(HttpMethod.Get, relative, eventStream: false, cancellationToken)
            .ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new KafkaUiException(FormatHttpError(response.StatusCode, relative, body));
        }

        if (string.IsNullOrWhiteSpace(body))
        {
            using var empty = JsonDocument.Parse("{}");
            return empty.RootElement.Clone();
        }

        try
        {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            throw new KafkaUiException("Resposta do Kafka UI não é JSON em " + PathOnly(relative) + ".");
        }
    }

    private async Task<HttpResponseMessage> SendApiAsync(
        HttpMethod method,
        string relative,
        bool eventStream,
        CancellationToken cancellationToken)
    {
        await EnsureLoginAsync(force: false, cancellationToken).ConfigureAwait(false);
        var response = await SendRawAsync(method, relative, null, api: true, eventStream, cancellationToken)
            .ConfigureAwait(false);
        if ((int)response.StatusCode != 401)
        {
            return response;
        }

        response.Dispose();
        _cookies.Clear();
        _bearer = null;
        _formSession = false;
        _loginAttempted = false;
        await EnsureLoginAsync(force: true, cancellationToken).ConfigureAwait(false);
        return await SendRawAsync(method, relative, null, api: true, eventStream, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task EnsureLoginAsync(bool force, CancellationToken cancellationToken)
    {
        if (_loginAttempted && !force)
        {
            return;
        }

        await _loginLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_loginAttempted && !force)
            {
                return;
            }

            await TryFormLoginAsync(cancellationToken).ConfigureAwait(false);
            _loginAttempted = true;
        }
        finally
        {
            _loginLock.Release();
        }
    }

    private async Task TryFormLoginAsync(CancellationToken cancellationToken)
    {
        if (!_config.HasCredentials)
        {
            _authMode = "anonimo";
            return;
        }

        foreach (var path in new[] { "auth", "login" })
        {
            using var content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["username"] = _config.User!,
                ["password"] = _config.Password!,
            });
            using var response = await SendRawAsync(HttpMethod.Post, path, content, api: false, eventStream: false, cancellationToken)
                .ConfigureAwait(false);
            CaptureCookies(response);
            var preview = await ReadPreviewAsync(response, cancellationToken).ConfigureAwait(false);
            CaptureBearer(preview);
            if (!IsLoginSuccess(response, preview))
            {
                continue;
            }

            _formSession = _cookies.Count > 0 || _bearer is not null;
            _authMode = "formulario POST /" + path;
            if (_formSession)
            {
                return;
            }
        }

        _authMode = "basic";
    }

    private async Task<HttpResponseMessage> SendRawAsync(
        HttpMethod method,
        string relative,
        HttpContent? content,
        bool api,
        bool eventStream,
        CancellationToken cancellationToken)
    {
        var request = new HttpRequestMessage(method, BuildUri(relative))
        {
            Content = content,
        };
        if (eventStream)
        {
            request.Headers.Accept.Clear();
            request.Headers.Accept.ParseAdd("text/event-stream");
        }

        if (api)
        {
            ApplyCredentials(request);
        }

        return await _http.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken).ConfigureAwait(false);
    }

    private void ApplyCredentials(HttpRequestMessage request)
    {
        if (_cookies.Count > 0)
        {
            request.Headers.TryAddWithoutValidation(
                "Cookie",
                string.Join("; ", _cookies.Select(pair => pair.Key + "=" + pair.Value)));
        }

        if (!string.IsNullOrWhiteSpace(_bearer))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _bearer);
            return;
        }

        if (!_formSession && _config.HasCredentials)
        {
            var raw = Convert.ToBase64String(Encoding.UTF8.GetBytes(_config.User + ":" + _config.Password));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", raw);
        }
    }

    private Uri BuildUri(string relative)
    {
        var root = _config.BaseUrl!.TrimEnd('/');
        return new Uri(root + "/" + relative.TrimStart('/'));
    }

    private void CaptureCookies(HttpResponseMessage response)
    {
        foreach (var header in response.Headers.NonValidated)
        {
            if (!header.Key.Equals("Set-Cookie", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var value in header.Value)
            {
                var pair = value.Split(';', 2)[0].Trim();
                var eq = pair.IndexOf('=');
                if (eq <= 0)
                {
                    continue;
                }

                var name = pair[..eq].Trim();
                var cookieValue = pair[(eq + 1)..].Trim();
                if (cookieValue.Length == 0)
                {
                    _cookies.Remove(name);
                    continue;
                }

                _cookies[name] = cookieValue;
            }
        }
    }

    private void CaptureBearer(string body)
    {
        var trimmed = body.Trim();
        if (!trimmed.StartsWith('{'))
        {
            return;
        }

        try
        {
            using var doc = JsonDocument.Parse(trimmed);
            var token = JsonText.ReadString(doc.RootElement, "token", "accessToken", "jweToken");
            if (!string.IsNullOrWhiteSpace(token))
            {
                _bearer = token;
            }
        }
        catch (JsonException)
        {
            // login sem JSON
        }
    }

    private static bool IsLoginSuccess(HttpResponseMessage response, string body)
    {
        var status = (int)response.StatusCode;
        if (status is 401 or 403 or 404 or 405)
        {
            return false;
        }

        if (response.Headers.Location is { } location &&
            location.ToString().Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (body.Contains("<form", StringComparison.OrdinalIgnoreCase) &&
            body.Contains("password", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return status is 200 or 204 or 302 or 303;
    }

    private static async Task<string> ReadPreviewAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.Content is null)
        {
            return "";
        }

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var buffer = new byte[2048];
        var read = await stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        return Encoding.UTF8.GetString(buffer, 0, read);
    }

    private string FormatHttpError(HttpStatusCode status, string relative, string body)
    {
        var safeBody = Redact(body);
        var code = (int)status;
        var hint = code switch
        {
            401 or 403 => "Credenciais recusadas ou sessão expirada. Confira KAFKA_UI_USER e KAFKA_UI_PASSWORD.",
            404 => "Não encontrado. Confira o cluster, o nome do tópico (parametrização da esteira / MCP SQL) e a URL base.",
            _ => "Falha HTTP " + code + ".",
        };

        return hint + " url=" + PathOnly(relative) + " corpo=" + Trim(safeBody);
    }

    private string Redact(string body)
    {
        if (string.IsNullOrEmpty(body))
        {
            return "";
        }

        if (!string.IsNullOrEmpty(_config.Password) &&
            body.Contains(_config.Password, StringComparison.Ordinal))
        {
            return "(corpo omitido)";
        }

        return body;
    }

    private string? RequireUrl() =>
        string.IsNullOrWhiteSpace(_config.BaseUrl)
            ? "Bloqueado: defina KAFKA_UI_URL. A página /auth é só o login; o MCP usa a raiz do portal."
            : null;

    private static List<ClusterInfo> ParseClusters(JsonElement json)
    {
        var source = json.ValueKind switch
        {
            JsonValueKind.Array => json,
            JsonValueKind.Object when json.TryGetProperty("clusters", out var nested) &&
                                       nested.ValueKind == JsonValueKind.Array => nested,
            JsonValueKind.Object when json.TryGetProperty("name", out _) => json,
            _ => default,
        };

        var list = new List<ClusterInfo>();
        if (source.ValueKind == JsonValueKind.Object && source.TryGetProperty("name", out _))
        {
            list.Add(ReadCluster(source));
            return list;
        }

        if (source.ValueKind != JsonValueKind.Array)
        {
            return list;
        }

        foreach (var item in source.EnumerateArray())
        {
            if (item.ValueKind == JsonValueKind.Object && item.TryGetProperty("name", out _))
            {
                list.Add(ReadCluster(item));
            }
        }

        return list;
    }

    private static ClusterInfo ReadCluster(JsonElement item) =>
        new(
            JsonText.ReadString(item, "name"),
            item.TryGetProperty("defaultCluster", out var flag) && flag.ValueKind == JsonValueKind.True,
            JsonText.ReadString(item, "status"),
            (int?)JsonText.ReadLong(item, "brokerCount"),
            (int?)JsonText.ReadLong(item, "topicCount"));

    private static string FormatCluster(ClusterInfo cluster) =>
        "- " + cluster.Name +
        " status=" + (string.IsNullOrEmpty(cluster.Status) ? "-" : cluster.Status) +
        " default=" + (cluster.DefaultCluster ? "true" : "false") +
        " brokers=" + (cluster.BrokerCount?.ToString() ?? "-") +
        " topicos=" + (cluster.TopicCount?.ToString() ?? "-");

    private static string FormatTopics(string cluster, string busca, JsonElement json)
    {
        var pageCount = json.TryGetProperty("pageCount", out var pages) && pages.TryGetInt32(out var count)
            ? count
            : 1;
        if (!json.TryGetProperty("topics", out var topics) || topics.ValueKind != JsonValueKind.Array)
        {
            return "cluster=" + cluster + "\nNenhum tópico na resposta.";
        }

        var lines = new List<string>
        {
            "cluster=" + cluster,
            "busca=" + (busca.Length == 0 ? "(nenhuma)" : busca),
            "paginas=" + pageCount,
        };
        var shown = 0;
        foreach (var topic in topics.EnumerateArray())
        {
            shown++;
            lines.Add(FormatTopicLine(topic));
        }

        lines.Insert(2, "mostrados=" + shown);
        if (pageCount > 1)
        {
            lines.Add("Refine a busca. A listagem está na página 1 de " + pageCount + ".");
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static string FormatTopicLine(JsonElement topic) =>
        "- " + JsonText.ReadString(topic, "name") +
        " partitions=" + (JsonText.ReadLong(topic, "partitionCount")?.ToString() ?? "-") +
        " replicas=" + (JsonText.ReadLong(topic, "replicationFactor")?.ToString() ?? "-") +
        " internos=" + (topic.TryGetProperty("internal", out var intern) && intern.ValueKind == JsonValueKind.True ? "true" : "false") +
        " cleanup=" + JsonText.ReadString(topic, "cleanUpPolicy") +
        " underReplicated=" + (JsonText.ReadLong(topic, "underReplicatedPartitions")?.ToString() ?? "-");

    private static string FormatTopicDetail(string cluster, JsonElement topic)
    {
        var lines = new List<string>
        {
            "cluster=" + cluster,
            "topico=" + JsonText.ReadString(topic, "name"),
            "interno=" + (topic.TryGetProperty("internal", out var intern) && intern.ValueKind == JsonValueKind.True ? "true" : "false"),
            "partitions=" + (JsonText.ReadLong(topic, "partitionCount")?.ToString() ?? "-"),
            "replicas=" + (JsonText.ReadLong(topic, "replicationFactor")?.ToString() ?? "-"),
            "inSyncReplicas=" + (JsonText.ReadLong(topic, "inSyncReplicas")?.ToString() ?? "-"),
            "underReplicated=" + (JsonText.ReadLong(topic, "underReplicatedPartitions")?.ToString() ?? "-"),
            "segmentSize=" + (JsonText.ReadLong(topic, "segmentSize")?.ToString() ?? "-"),
            "cleanup=" + JsonText.ReadString(topic, "cleanUpPolicy"),
        };
        return string.Join(Environment.NewLine, lines);
    }

    private static string PathOnly(string relative)
    {
        var cut = relative.IndexOf('?');
        return cut < 0 ? relative : relative[..cut];
    }

    private static string Trim(string? content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return "";
        }

        var trimmed = content.Trim().ReplaceLineEndings(" ");
        return trimmed.Length <= 400 ? trimmed : trimmed[..400] + "...";
    }

    private static HttpMessageHandler CreateHandler(KafkaConfig config)
    {
        var handler = new HttpClientHandler
        {
            UseCookies = false,
            AllowAutoRedirect = false,
        };
        if (config.SkipTlsVerify)
        {
            handler.ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
        }

        return handler;
    }

    private sealed record ClusterInfo(
        string Name,
        bool DefaultCluster,
        string Status,
        int? BrokerCount,
        int? TopicCount);
}

namespace McpKafka.Services;

public sealed class KafkaConfig
{
    public KafkaConfig()
        : this(
            NormalizeBaseUrl(FirstNonEmpty(
                Environment.GetEnvironmentVariable("KAFKA_UI_URL"),
                Environment.GetEnvironmentVariable("MCP_KAFKA_URL"))),
            FirstNonEmpty(
                Environment.GetEnvironmentVariable("KAFKA_UI_USER"),
                Environment.GetEnvironmentVariable("MCP_KAFKA_USER")),
            FirstNonEmpty(
                Environment.GetEnvironmentVariable("KAFKA_UI_PASSWORD"),
                Environment.GetEnvironmentVariable("MCP_KAFKA_PASSWORD")),
            FirstNonEmpty(
                Environment.GetEnvironmentVariable("KAFKA_CLUSTER"),
                Environment.GetEnvironmentVariable("MCP_KAFKA_CLUSTER")),
            GetBool("KAFKA_SKIP_TLS_VERIFY") || GetBool("KAFKA_INSECURE_SKIP_TLS_VERIFY"),
            Clamp(GetInt("KAFKA_MAX_MESSAGES", 100), 1, 200),
            Clamp(GetInt("KAFKA_TIMEOUT_SECONDS", 90), 5, 300))
    {
    }

    internal KafkaConfig(
        string? baseUrl,
        string? user,
        string? password,
        string? cluster,
        bool skipTlsVerify,
        int maxMessages,
        int timeoutSeconds)
    {
        BaseUrl = NormalizeBaseUrl(baseUrl);
        User = EmptyToNull(user);
        Password = EmptyToNull(password);
        Cluster = EmptyToNull(cluster);
        SkipTlsVerify = skipTlsVerify;
        MaxMessages = Clamp(maxMessages, 1, 200);
        TimeoutSeconds = Clamp(timeoutSeconds, 5, 300);
    }

    public string? BaseUrl { get; }

    public string? User { get; }

    public string? Password { get; }

    public string? Cluster { get; }

    public bool SkipTlsVerify { get; }

    public int MaxMessages { get; }

    public int TimeoutSeconds { get; }

    public bool HasCredentials =>
        !string.IsNullOrWhiteSpace(User) && !string.IsNullOrWhiteSpace(Password);

    /// <summary>
    /// A URL que a pessoa cola é a página de login (/auth ou /login). A API fica na raiz.
    /// </summary>
    public static string? NormalizeBaseUrl(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var server = raw.Trim();
        var hash = server.IndexOf('#');
        if (hash >= 0)
        {
            server = server[..hash];
        }

        var query = server.IndexOf('?');
        if (query >= 0)
        {
            server = server[..query];
        }

        server = server.TrimEnd('/');
        if (server.EndsWith("/auth", StringComparison.OrdinalIgnoreCase))
        {
            server = server[..^"/auth".Length];
        }
        else if (server.EndsWith("/login", StringComparison.OrdinalIgnoreCase))
        {
            server = server[..^"/login".Length];
        }

        server = server.TrimEnd('/');
        return string.IsNullOrWhiteSpace(server) ? null : server;
    }

    private static bool GetBool(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return !string.IsNullOrWhiteSpace(value) &&
               !string.Equals(value, "false", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(value, "0", StringComparison.OrdinalIgnoreCase);
    }

    private static int GetInt(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, out var parsed) ? parsed : defaultValue;
    }

    private static int Clamp(int value, int min, int max)
    {
        if (value < min)
        {
            return min;
        }

        return value > max ? max : value;
    }

    private static string? EmptyToNull(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string? FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }
}

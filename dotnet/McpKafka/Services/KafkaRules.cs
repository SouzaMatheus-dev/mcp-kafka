namespace McpKafka.Services;

internal static class KafkaRules
{
    public static string? ValidateTopic(string? topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return "Bloqueado: informe o tópico. O nome está na parametrização das fases da esteira (MCP SQL).";
        }

        var name = topic.Trim();
        if (name.Length > 249 || name.Contains('/') || name.Contains('\\') || name.Any(char.IsControl))
        {
            return "Bloqueado: nome de tópico inválido.";
        }

        return null;
    }

    public static string? ValidateFilter(string? value, string label)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        if (value.Length > 256 || value.Any(char.IsControl))
        {
            return "Bloqueado: " + label + " excede 256 caracteres ou contém caractere de controle.";
        }

        return null;
    }

    public static bool TryMapOrigem(
        string? origem,
        out string seekType,
        out string seekDirection,
        out string normalizada,
        out string error)
    {
        var value = string.IsNullOrWhiteSpace(origem) ? "recentes" : origem.Trim();
        switch (value.ToLowerInvariant())
        {
            case "recentes":
            case "latest":
            case "newest":
            case "fim":
                seekType = "LATEST";
                seekDirection = "BACKWARD";
                normalizada = "recentes";
                error = "";
                return true;
            case "inicio":
            case "início":
            case "beginning":
            case "oldest":
                seekType = "BEGINNING";
                seekDirection = "FORWARD";
                normalizada = "inicio";
                error = "";
                return true;
            default:
                seekType = "";
                seekDirection = "";
                normalizada = "";
                error = "Bloqueado: origem deve ser recentes ou inicio. O modo ao vivo (TAILING) fica de fora para o stream não permanecer aberto.";
                return false;
        }
    }
}

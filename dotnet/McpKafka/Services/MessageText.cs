using System.Text;
using System.Text.Json;

namespace McpKafka.Services;

internal static class JsonText
{
    public static string ReadString(JsonElement element, params string[] names)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            return "";
        }

        foreach (var name in names)
        {
            if (!element.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            return value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : value.GetRawText();
        }

        return "";
    }

    public static long? ReadLong(JsonElement element, string name)
    {
        if (element.ValueKind != JsonValueKind.Object ||
            !element.TryGetProperty(name, out var value) ||
            value.ValueKind != JsonValueKind.Number ||
            !value.TryGetInt64(out var number))
        {
            return null;
        }

        return number;
    }

    public static string ReadHeaders(JsonElement message)
    {
        if (!message.TryGetProperty("headers", out var headers) || headers.ValueKind != JsonValueKind.Object)
        {
            return "";
        }

        var parts = new List<string>();
        foreach (var header in headers.EnumerateObject())
        {
            if (parts.Count == 12)
            {
                parts.Add("...");
                break;
            }

            var text = header.Value.ValueKind == JsonValueKind.String
                ? header.Value.GetString() ?? ""
                : header.Value.GetRawText();
            parts.Add(header.Name + "=" + text);
        }

        return string.Join(", ", parts);
    }
}

internal static class MessageText
{
    private const int MaxContent = 6000;

    public static string FormatBusca(
        string cluster,
        string topic,
        string? filtro,
        string origem,
        string seekType,
        string seekDirection,
        MessageCapture capture)
    {
        var sb = new StringBuilder();
        sb.AppendLine("modo=somente_leitura");
        sb.AppendLine("cluster=" + cluster);
        sb.AppendLine("topico=" + topic);
        sb.AppendLine("origem=" + origem + " seekType=" + seekType + " seekDirection=" + seekDirection);
        sb.AppendLine("filtro=" + (string.IsNullOrEmpty(filtro) ? "(nenhum — janela sem texto)" : filtro));
        if (!string.IsNullOrWhiteSpace(capture.Phase))
        {
            sb.AppendLine("fase=" + capture.Phase);
        }

        if (capture.MessagesConsumed is not null)
        {
            sb.AppendLine(
                "varredura_kafka_ui=mensagens_lidas=" + capture.MessagesConsumed +
                " ms=" + (capture.ElapsedMs?.ToString() ?? "-") +
                " erros_filtro=" + (capture.FilterErrors?.ToString() ?? "0"));
        }

        sb.AppendLine("encontradas=" + capture.Messages.Count);
        if (capture.Done)
        {
            sb.AppendLine("stream=DONE");
        }

        var index = 1;
        foreach (var message in capture.Messages)
        {
            sb.AppendLine();
            sb.AppendLine("--- mensagem " + index + " ---");
            sb.AppendLine(
                "partition=" + message.Partition +
                " offset=" + message.Offset +
                " timestamp=" + message.Timestamp);
            if (!string.IsNullOrEmpty(message.Key))
            {
                sb.AppendLine("key=" + message.Key);
            }

            if (!string.IsNullOrEmpty(message.ValueSerde))
            {
                sb.AppendLine("valueSerde=" + message.ValueSerde);
            }

            if (!string.IsNullOrEmpty(message.Headers))
            {
                sb.AppendLine("headers=" + message.Headers);
            }

            sb.AppendLine("conteudo:");
            sb.AppendLine(Pretty(message.Content));
            index++;
        }

        if (capture.Messages.Count == 0)
        {
            sb.AppendLine();
            sb.AppendLine("Nenhuma mensagem nesta janela.");
            sb.AppendLine("O Kafka UI aplica STRING_CONTAINS só na janela pedida. Não é índice do tópico inteiro.");
            sb.AppendLine("Se o ProcessoID for antigo, chame de novo com origem=inicio.");
            sb.AppendLine("O nome do tópico sai da parametrização das fases da esteira (MCP SQL).");
        }

        return sb.ToString().TrimEnd();
    }

    public static string Pretty(string content)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return "(vazio)";
        }

        var text = content.Trim();
        if (text.StartsWith('{') || text.StartsWith('['))
        {
            try
            {
                using var doc = JsonDocument.Parse(text);
                text = JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions { WriteIndented = true });
            }
            catch (JsonException)
            {
                // conteúdo parece JSON e não é — devolve o texto cru
            }
        }

        if (text.Length <= MaxContent)
        {
            return text;
        }

        return text[..MaxContent] + "... [conteudo truncado]";
    }
}

using System.Text;
using System.Text.Json;

namespace McpKafka.Services;

internal sealed record KafkaMessage(
    int Partition,
    long Offset,
    string Timestamp,
    string Key,
    string Content,
    string Headers,
    string ValueSerde)
{
    public bool Contains(string text) =>
        Key.Contains(text, StringComparison.Ordinal) ||
        Content.Contains(text, StringComparison.Ordinal) ||
        Headers.Contains(text, StringComparison.Ordinal);
}

internal sealed class MessageCapture
{
    public List<KafkaMessage> Messages { get; } = [];

    public string? Phase { get; set; }

    public long? MessagesConsumed { get; set; }

    public long? ElapsedMs { get; set; }

    public int? FilterErrors { get; set; }

    public bool Done { get; set; }

    public int IgnoredByLocalFilter { get; set; }
}

internal static class SseParser
{
    public static MessageCapture CollectBody(string body, int limit, string? mustContain)
    {
        var capture = new MessageCapture();
        var trimmed = body.Trim();
        if (trimmed.Length == 0 || limit < 1)
        {
            return capture;
        }

        if (trimmed.StartsWith("data:", StringComparison.OrdinalIgnoreCase) ||
            trimmed.Contains("\n\n", StringComparison.Ordinal) ||
            trimmed.Contains("\ndata:", StringComparison.OrdinalIgnoreCase))
        {
            CollectRawSse(trimmed, capture, limit, mustContain);
            return capture;
        }

        if (trimmed.StartsWith('['))
        {
            using var doc = JsonDocument.Parse(trimmed);
            foreach (var item in doc.RootElement.EnumerateArray())
            {
                Apply(item, capture, limit, mustContain);
                if (Stop(capture, limit))
                {
                    break;
                }
            }

            return capture;
        }

        foreach (var line in body.Split('\n'))
        {
            var candidate = line.Trim();
            if (!candidate.StartsWith('{'))
            {
                continue;
            }

            using var doc = JsonDocument.Parse(candidate);
            Apply(doc.RootElement, capture, limit, mustContain);
            if (Stop(capture, limit))
            {
                break;
            }
        }

        return capture;
    }

    public static async Task<MessageCapture> CollectStreamAsync(
        Stream stream,
        int limit,
        string? mustContain,
        CancellationToken cancellationToken)
    {
        var capture = new MessageCapture();
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var data = new StringBuilder();

        while (!Stop(capture, limit))
        {
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                FlushEvent(data, capture, limit, mustContain);
                break;
            }

            if (line.Length == 0)
            {
                FlushEvent(data, capture, limit, mustContain);
                continue;
            }

            if (line.StartsWith(':'))
            {
                continue;
            }

            if (line.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
            {
                if (data.Length > 0)
                {
                    data.Append('\n');
                }

                data.Append(line[5..].TrimStart());
            }
        }

        return capture;
    }

    private static void CollectRawSse(string raw, MessageCapture capture, int limit, string? mustContain)
    {
        var data = new StringBuilder();
        foreach (var line in raw.Split('\n'))
        {
            var current = line.TrimEnd('\r');
            if (current.Length == 0)
            {
                FlushEvent(data, capture, limit, mustContain);
                if (Stop(capture, limit))
                {
                    return;
                }

                continue;
            }

            if (current.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
            {
                if (data.Length > 0)
                {
                    data.Append('\n');
                }

                data.Append(current[5..].TrimStart());
            }
        }

        FlushEvent(data, capture, limit, mustContain);
    }

    private static void FlushEvent(StringBuilder data, MessageCapture capture, int limit, string? mustContain)
    {
        if (data.Length == 0)
        {
            return;
        }

        var payload = data.ToString();
        data.Clear();
        try
        {
            using var doc = JsonDocument.Parse(payload);
            Apply(doc.RootElement, capture, limit, mustContain);
        }
        catch (JsonException)
        {
            // evento que não é JSON — ignora e segue o stream
        }
    }

    private static void Apply(JsonElement evt, MessageCapture capture, int limit, string? mustContain)
    {
        if (evt.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        if (evt.TryGetProperty("type", out var typeProp) && typeProp.ValueKind == JsonValueKind.String)
        {
            var type = typeProp.GetString() ?? "";
            if (type.Equals("PHASE", StringComparison.OrdinalIgnoreCase) &&
                evt.TryGetProperty("phase", out var phase))
            {
                capture.Phase = JsonText.ReadString(phase, "name");
            }
            else if (type.Equals("CONSUMING", StringComparison.OrdinalIgnoreCase) &&
                     evt.TryGetProperty("consuming", out var consuming))
            {
                capture.MessagesConsumed = JsonText.ReadLong(consuming, "messagesConsumed");
                capture.ElapsedMs = JsonText.ReadLong(consuming, "elapsedMs");
                capture.FilterErrors = (int?)JsonText.ReadLong(consuming, "filterApplyErrors");
            }
            else if (type.Equals("DONE", StringComparison.OrdinalIgnoreCase))
            {
                capture.Done = true;
            }
        }

        if (capture.Messages.Count >= limit || !TryReadMessage(evt, out var message))
        {
            return;
        }

        if (!string.IsNullOrEmpty(mustContain) && !message.Contains(mustContain))
        {
            capture.IgnoredByLocalFilter++;
            return;
        }

        capture.Messages.Add(message);
    }

    private static bool TryReadMessage(JsonElement evt, out KafkaMessage message)
    {
        if (evt.TryGetProperty("type", out var typeProp) &&
            typeProp.ValueKind == JsonValueKind.String &&
            string.Equals(typeProp.GetString(), "MESSAGE", StringComparison.OrdinalIgnoreCase) &&
            evt.TryGetProperty("message", out var nested) &&
            nested.ValueKind == JsonValueKind.Object)
        {
            message = ReadMessage(nested);
            return true;
        }

        if (evt.TryGetProperty("partition", out _) && evt.TryGetProperty("offset", out _))
        {
            message = ReadMessage(evt);
            return true;
        }

        message = new KafkaMessage(0, 0, "", "", "", "", "");
        return false;
    }

    private static KafkaMessage ReadMessage(JsonElement msg)
    {
        var content = JsonText.ReadString(msg, "content", "value");
        return new KafkaMessage(
            (int)(JsonText.ReadLong(msg, "partition") ?? 0),
            JsonText.ReadLong(msg, "offset") ?? 0,
            JsonText.ReadString(msg, "timestamp"),
            JsonText.ReadString(msg, "key"),
            content,
            JsonText.ReadHeaders(msg),
            JsonText.ReadString(msg, "valueSerde", "valueFormat"));
    }

    private static bool Stop(MessageCapture capture, int limit) =>
        capture.Done || capture.Messages.Count >= limit;
}

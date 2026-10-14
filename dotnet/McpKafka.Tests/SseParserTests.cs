using McpKafka.Services;

namespace McpKafka.Tests;

public class SseParserTests
{
    [Fact]
    public void LeMensagemDoStreamComProcessoId()
    {
        const string sse =
            "data: {\"type\":\"PHASE\",\"phase\":{\"name\":\"Consumer assigned\"}}\n\n" +
            "data: {\"type\":\"CONSUMING\",\"consuming\":{\"messagesConsumed\":15,\"elapsedMs\":40,\"bytesConsumed\":9,\"isCancelled\":false,\"filterApplyErrors\":0}}\n\n" +
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":2,\"offset\":99,\"timestamp\":\"2026-10-14T12:00:00Z\",\"key\":\"k\",\"content\":\"{\\\"ProcessoID\\\":\\\"PROC-42\\\"}\",\"headers\":{\"origem\":\"saf\"},\"valueSerde\":\"String\"}}\n\n" +
            "data: {\"type\":\"DONE\"}\n\n";

        var capture = SseParser.CollectBody(sse, limit: 10, mustContain: "PROC-42");

        Assert.True(capture.Done);
        Assert.Equal("Consumer assigned", capture.Phase);
        Assert.Equal(15, capture.MessagesConsumed);
        var message = Assert.Single(capture.Messages);
        Assert.Equal(2, message.Partition);
        Assert.Equal(99, message.Offset);
        Assert.Contains("PROC-42", message.Content, StringComparison.Ordinal);
        Assert.Contains("origem=saf", message.Headers, StringComparison.Ordinal);
    }

    [Fact]
    public void AceitaCampoValueQuandoNaoHaContent()
    {
        const string sse =
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":0,\"offset\":1,\"timestamp\":\"2026-10-14T12:00:00Z\",\"value\":\"{\\\"ProcessoID\\\":\\\"ABC\\\"}\"}}\n\n";

        var capture = SseParser.CollectBody(sse, limit: 5, mustContain: "ABC");

        Assert.Contains("ABC", Assert.Single(capture.Messages).Content, StringComparison.Ordinal);
    }

    [Fact]
    public void FiltroLocalDescartaMensagemSemOProcesso()
    {
        const string sse =
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":0,\"offset\":1,\"timestamp\":\"2026-10-14T12:00:00Z\",\"content\":\"outro\"}}\n\n" +
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":0,\"offset\":2,\"timestamp\":\"2026-10-14T12:00:01Z\",\"content\":\"ProcessoID=PROC-7\"}}\n\n";

        var capture = SseParser.CollectBody(sse, limit: 10, mustContain: "PROC-7");

        Assert.Equal(1, capture.IgnoredByLocalFilter);
        Assert.Equal(2, Assert.Single(capture.Messages).Offset);
    }

    [Fact]
    public async Task StreamParaNoLimite()
    {
        const string sse =
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":0,\"offset\":1,\"timestamp\":\"t\",\"content\":\"a\"}}\n\n" +
            "data: {\"type\":\"MESSAGE\",\"message\":{\"partition\":0,\"offset\":2,\"timestamp\":\"t\",\"content\":\"b\"}}\n\n";
        await using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(sse));

        var capture = await SseParser.CollectStreamAsync(stream, limit: 1, mustContain: null, CancellationToken.None);

        var message = Assert.Single(capture.Messages);
        Assert.Equal(1, message.Offset);
    }
}

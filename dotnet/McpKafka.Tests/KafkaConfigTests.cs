using McpKafka.Services;

namespace McpKafka.Tests;

public class KafkaConfigTests
{
    [Theory]
    [InlineData("https://kafka-ui-a.exemplo.interno/auth", "https://kafka-ui-a.exemplo.interno")]
    [InlineData("https://kafka-ui-b.exemplo.interno/auth/", "https://kafka-ui-b.exemplo.interno")]
    [InlineData("https://kafka.exemplo/login", "https://kafka.exemplo")]
    [InlineData("https://kafka.exemplo/auth?logout", "https://kafka.exemplo")]
    [InlineData("https://kafka.exemplo/#/auth", "https://kafka.exemplo")]
    [InlineData("https://kafka.exemplo", "https://kafka.exemplo")]
    public void UrlDeLoginViraRaizDaApi(string bruto, string esperado)
    {
        Assert.Equal(esperado, KafkaConfig.NormalizeBaseUrl(bruto));
    }

    [Fact]
    public void TetoDeMensagensRespeitaOAmbiente()
    {
        var anterior = Environment.GetEnvironmentVariable("KAFKA_MAX_MESSAGES");
        try
        {
            Environment.SetEnvironmentVariable("KAFKA_MAX_MESSAGES", "12");
            var config = new KafkaConfig();
            Assert.Equal(12, config.MaxMessages);
        }
        finally
        {
            Environment.SetEnvironmentVariable("KAFKA_MAX_MESSAGES", anterior);
        }
    }

    [Fact]
    public void TetoDeMensagensNaoPassaDeDuzentas()
    {
        var anterior = Environment.GetEnvironmentVariable("KAFKA_MAX_MESSAGES");
        try
        {
            Environment.SetEnvironmentVariable("KAFKA_MAX_MESSAGES", "9999");
            var config = new KafkaConfig();
            Assert.Equal(200, config.MaxMessages);
        }
        finally
        {
            Environment.SetEnvironmentVariable("KAFKA_MAX_MESSAGES", anterior);
        }
    }
}

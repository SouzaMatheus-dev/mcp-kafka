using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using McpKafka.Services;
using McpKafka.Tools;

var builder = Host.CreateEmptyApplicationBuilder(settings: null);

builder.Logging.AddConsole(options =>
{
    options.LogToStandardErrorThreshold = LogLevel.Trace;
});

builder.Services.AddSingleton<KafkaConfig>();
builder.Services.AddSingleton<KafkaUiClient>();
builder.Services.AddSingleton<KafkaTools>();
builder.Services
    .AddMcpServer(options =>
    {
        options.ServerInstructions =
            "Especialista em Kafka UI (somente leitura). " +
            "O nome do tópico vem da parametrização das fases da esteira: consulte o MCP SQL antes. " +
            "Depois: 1) contexto_atual ou listar_clusters, " +
            "2) listar_topicos com a busca do nome, " +
            "3) buscar_mensagens com o tópico e o ProcessoID. " +
            "Padrão da busca é a janela recente. Se não achar, repita com origem=inicio. " +
            "Não produza, não apague e não altere tópico. Credencial só no ambiente do cliente.";
    })
    .WithStdioServerTransport()
    .WithToolsFromAssembly();

await builder.Build().RunAsync();

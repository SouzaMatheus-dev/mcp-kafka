using System.ComponentModel;
using System.Net.Sockets;
using McpKafka.Services;
using ModelContextProtocol.Server;

namespace McpKafka.Tools;

[McpServerToolType]
public sealed class KafkaTools(KafkaConfig config, KafkaUiClient client)
{
    [McpServerTool(Name = "contexto_atual")]
    [Description("Mostra o portal Kafka UI, o modo somente leitura e os clusters visíveis. Não revela senha.")]
    public Task<string> ContextoAtual() => Safe(() => client.DescreverContextoAsync());

    [McpServerTool(Name = "diagnosticar_acesso")]
    [Description("Testa login e a API de clusters/tópicos. Use quando as outras tools falharem por rede, TLS ou credencial.")]
    public Task<string> DiagnosticarAcesso() => Safe(() => client.DiagnosticarAsync());

    [McpServerTool(Name = "listar_clusters")]
    [Description("Lista os clusters Kafka expostos pelo Kafka UI (nome, status, quantidade de tópicos).")]
    public Task<string> ListarClusters() => Safe(() => client.ListarClustersAsync());

    [McpServerTool(Name = "listar_topicos")]
    [Description("Lista tópicos do cluster. Informe busca para filtrar pelo nome (substring). O nome oficial sai da parametrização da esteira no MCP SQL.")]
    public Task<string> ListarTopicos(string busca = "", string cluster = "", bool incluirInternos = false) =>
        Safe(() => client.ListarTopicosAsync(busca, cluster, incluirInternos));

    [McpServerTool(Name = "obter_topico")]
    [Description("Detalhe de um tópico: partições, réplicas, under-replicated e cleanup. Somente leitura.")]
    public Task<string> ObterTopico(string topico, string cluster = "") =>
        Safe(() => client.ObterTopicoAsync(topico, cluster));

    [McpServerTool(Name = "buscar_mensagens")]
    [Description("Lê mensagens de um tópico no Kafka UI e localiza pelo ProcessoID. Equivale a Topics → filtro do tópico → Messages. Padrão: janela recente (LATEST/BACKWARD) com STRING_CONTAINS. origem=inicio varre a partir do começo. Sem produce e sem delete.")]
    public Task<string> BuscarMensagens(
        string topico,
        string processo_id = "",
        string texto = "",
        int limite = 20,
        string origem = "recentes",
        string cluster = "") =>
        Safe(() => client.BuscarMensagensAsync(topico, processo_id, texto, limite, origem, cluster));

    private async Task<string> Safe(Func<Task<string>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (KafkaUiException ex)
        {
            return ex.Message;
        }
        catch (Exception ex) when (IsDns(ex))
        {
            return "Falha de rede: o DNS do Kafka UI não resolveu. Confira a VPN corporativa e KAFKA_UI_URL. Detalhe: " +
                   ex.Message;
        }
        catch (TaskCanceledException)
        {
            return "Tempo esgotado ao falar com o Kafka UI (" + config.TimeoutSeconds + "s).";
        }
        catch (HttpRequestException ex)
        {
            return "Falha HTTP ao falar com o Kafka UI: " + ex.Message;
        }
        catch (Exception ex)
        {
            return "Falha: " + ex.Message;
        }
    }

    private static bool IsDns(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socket &&
                socket.SocketErrorCode is SocketError.HostNotFound or SocketError.TryAgain or SocketError.NoData)
            {
                return true;
            }
        }

        return false;
    }
}

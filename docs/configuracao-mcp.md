# Configuração MCP

Credencial só no ambiente do cliente. Cada portal é uma entrada. A senha de um
não vale no outro.

A URL pode ser a página de login (`.../auth`). O processo usa a raiz.

## Cliente MCP

O processo fala por **stdio**. A configuração abaixo entra no arquivo de servidores
MCP do cliente. Usuário e senha ficam só nesse arquivo, fora do repositório.

```json
{
  "mcpServers": {
    "kafka-a": {
      "command": "mcp-kafka",
      "timeout": 120000,
      "trust": false,
      "env": {
        "KAFKA_UI_URL": "https://kafka-ui-a.exemplo.interno/auth",
        "KAFKA_UI_USER": "",
        "KAFKA_UI_PASSWORD": "",
        "KAFKA_SKIP_TLS_VERIFY": "true"
      }
    },
    "kafka-b": {
      "command": "mcp-kafka",
      "timeout": 120000,
      "trust": false,
      "env": {
        "KAFKA_UI_URL": "https://kafka-ui-b.exemplo.interno/auth",
        "KAFKA_UI_USER": "",
        "KAFKA_UI_PASSWORD": "",
        "KAFKA_SKIP_TLS_VERIFY": "true"
      }
    }
  }
}
```

Reinicie o cliente. As tools que aparecem são só de leitura.

## Cluster explícito

Quando `/api/clusters` devolve mais de um cluster e nenhum é `defaultCluster`:

```json
"KAFKA_CLUSTER": "nome-do-cluster"
```

Ou passe `cluster` em cada tool.

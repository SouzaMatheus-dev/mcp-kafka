# Configuração MCP

Credencial só no ambiente do cliente. Cada portal é uma entrada. A senha de um
não vale no outro.

A URL pode ser a página de login (`.../auth`). O processo usa a raiz.

## Gemini CLI

Arquivo do usuário: `%USERPROFILE%\.gemini\settings.json`  
Segredos: `.gemini/settings.local.json` (gitignored).

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

Reinicie o Gemini e rode `/mcp list`. As tools são só de leitura.

## Cluster explícito

Quando `/api/clusters` devolve mais de um cluster e nenhum é `defaultCluster`:

```json
"KAFKA_CLUSTER": "nome-do-cluster"
```

Ou passe `cluster` em cada tool.

# McpKafka

[![NuGet](https://img.shields.io/nuget/v/McpKafka?logo=nuget&logoColor=white&label=NuGet&color=004880)](https://www.nuget.org/packages/McpKafka)
[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4?logo=dotnet&logoColor=white)](https://dotnet.microsoft.com/download/dotnet/8.0)
[![MCP](https://img.shields.io/badge/MCP-1.4-111111)](https://modelcontextprotocol.io)
[![License](https://img.shields.io/github/license/SouzaMatheus-dev/mcp-kafka?color=blue)](https://github.com/SouzaMatheus-dev/mcp-kafka/blob/main/LICENSE)

Servidor **MCP especialista em Kafka UI**, **somente leitura**.

Conecte o Gemini CLI ao portal Kafka UI. O MCP lista clusters e tópicos e
localiza mensagens pelo **ProcessoID**. Não produz, não apaga e não altera tópico.

O pacote **não embute** URL, usuário nem senha. Cada ambiente é uma entrada MCP,
com credencial só no `env` local.

Repositório: https://github.com/SouzaMatheus-dev/mcp-kafka

---

## O que este pacote faz

- Fala com a API do **Kafka UI** (Provectus 0.7 / Kafbat): `GET /api/clusters`, tópicos e mensagens
- Login igual ao da página `/auth`: `POST /auth` com formulário. Se o portal for mais novo, tenta `POST /login`. Se o formulário não existir, usa Basic
- Lista clusters e tópicos (filtro por nome)
- Lê mensagens em stream (`text/event-stream`) e filtra com `STRING_CONTAINS`
- Atalho do uso corporativo: **Topics → tópico → Messages → ProcessoID**

## O que este pacote não faz

- `POST` de mensagem (produce)
- delete de mensagem ou de tópico
- consumer ao vivo (`TAILING`), para o stream não ficar aberto
- guarda de senha, URL interna ou ProcessoID dentro do pacote

O nome do tópico **não está fixo aqui**. Ele sai da tabela de parametrização das
fases da esteira. Use o **MCP SQL** (scripts úteis) para achar o nome e só então
chame `buscar_mensagens`. |

---

## Requisitos

| Item | Detalhe |
| --- | --- |
| .NET | **8+** para global tool · **10+** para `dnx` |
| Portal | Kafka UI com login em `/auth` |
| Rede | HTTPS até o portal. Sem a VPN corporativa o DNS interno não resolve |

---

## Instalação

```powershell
dotnet tool install --global McpKafka
dotnet tool update --global McpKafka
```

O comando instalado é `mcp-kafka`.

Antes do pacote existir no NuGet.org, instale o `.nupkg` local:

```powershell
dotnet pack dotnet/McpKafka/McpKafka.csproj -c Release -o dotnet/nupkg
dotnet tool install --global --add-source dotnet/nupkg McpKafka
```

---

## Configuração

Uma entrada por portal. A URL pode ser a página de login (`/auth`): o MCP
corta esse sufixo e chama a API na raiz.

`KAFKA_SKIP_TLS_VERIFY=true` quando o certificado do ingress interno não está
na máquina.

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

Senha só em arquivo local gitignored (`settings.local.json` ou o `env` do cliente).
Modelo sem segredo: `docs/examples/gemini.json`.

Se o portal tiver mais de um cluster e nenhum marcado como padrão, defina
`KAFKA_CLUSTER` ou passe o parâmetro `cluster` na tool.

---

## Ferramentas

| Ferramenta | Parâmetros | O que devolve |
| --- | --- | --- |
| `contexto_atual` | — | portal, usuário (sem senha), clusters |
| `diagnosticar_acesso` | — | login, `/api/clusters`, amostra de tópicos |
| `listar_clusters` | — | nome, status, brokers, quantidade de tópicos |
| `listar_topicos` | `busca`, `cluster`, `incluirInternos` | página 1 de tópicos |
| `obter_topico` | `topico`, `cluster` | partições, réplicas, cleanup |
| `buscar_mensagens` | `topico`, `processo_id`, `texto`, `limite`, `origem`, `cluster` | mensagens da janela |

`origem=recentes` (padrão) usa `seekType=LATEST` e `seekDirection=BACKWARD`.
`origem=inicio` usa `BEGINNING` e `FORWARD`. O `limite` é o máximo de mensagens
**devolvidas** (teto `KAFKA_MAX_MESSAGES`, padrão 100, máximo 200).

O filtro é o mesmo da caixa de busca do menu Messages: `q` + `STRING_CONTAINS`.
Ele vale para a janela lida, não é um índice do tópico inteiro. Se o ProcessoID
não aparecer nas recentes, repita com `origem=inicio`.

---

## Fluxo

1. MCP SQL: nome do tópico na parametrização da fase
2. `listar_topicos` com um pedaço do nome, se quiser confirmar que ele existe
3. `buscar_mensagens` com o tópico e o ProcessoID
4. Se `encontradas=0`, chame de novo com `origem=inicio` e um `limite` maior

Pedido típico ao agente:

- “Ache a mensagem do ProcessoID X no tópico de pedidos.”
- “Liste os tópicos cujo nome contém pedidos.”

---

## Variáveis

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `KAFKA_UI_URL` | — | URL do portal. `/auth` e `/login` são removidos |
| `MCP_KAFKA_URL` | — | Alias |
| `KAFKA_UI_USER` / `KAFKA_UI_PASSWORD` | — | Login do Kafka UI. Nunca no git |
| `MCP_KAFKA_USER` / `MCP_KAFKA_PASSWORD` | — | Aliases |
| `KAFKA_CLUSTER` | descoberta | Cluster quando há mais de um |
| `KAFKA_SKIP_TLS_VERIFY` | `false` | Aceita certificado interno |
| `KAFKA_MAX_MESSAGES` | `100` | Teto do `limite` (1–200) |
| `KAFKA_TIMEOUT_SECONDS` | `90` | Tempo máximo da chamada, inclusive o stream |

---

## API usada

Contrato do Kafka UI 0.7 (o login em `/auth` é o `LOGIN_FORM` dessa linha):

- `POST /auth` (`application/x-www-form-urlencoded`, `username` e `password`) → cookie `SESSION`
- `GET /api/clusters`
- `GET /api/clusters/{cluster}/topics?search=&page=1&perPage=`
- `GET /api/clusters/{cluster}/topics/{topic}/messages?seekType=&seekDirection=&limit=&q=&filterQueryType=STRING_CONTAINS`
- Resposta de mensagens: `text/event-stream` com eventos `PHASE`, `CONSUMING`, `MESSAGE`, `DONE`
- O valor da mensagem vem em `message.content` (ou `message.value` em portais mais novos)

Portais Kafbat que trocaram o login para `POST /login` entram no segundo passo,
sem mudar as tools.

---

## Problemas comuns

| Sintoma | O que fazer |
| --- | --- |
| DNS não resolve | VPN. Fora dela o DNS interno não alcança o host |
| 401 | Usuário ou senha do portal. Cada entrada MCP tem a própria senha |
| 404 no tópico | Nome diferente do parâmetro da esteira. Confira no MCP SQL |
| `encontradas=0` | Janela recente não tinha o ProcessoID. Use `origem=inicio` |
| TLS | `KAFKA_SKIP_TLS_VERIFY=true` no env local |
| Vários clusters | Passe `cluster` ou defina `KAFKA_CLUSTER` |

## Licença

MIT

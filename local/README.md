# Ambiente local

As credenciais do Kafka UI ficam em `%USERPROFILE%\.gemini\settings.local.json` (gitignored no Gemini). O repositório não guarda usuário, senha nem URL interna.

Cada portal é uma entrada `mcpServers`. A URL com `/auth` é a página de login; o processo usa a raiz. `KAFKA_SKIP_TLS_VERIFY=true` cobre o certificado do ingress interno.

O DNS corporativo não resolve fora da VPN. O pacote foi validado contra um Kafka UI falso com o mesmo contrato (`POST /auth` e stream de mensagens).

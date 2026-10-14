# Exemplos

O nome do tópico vem da parametrização das fases da esteira (MCP SQL). Nos exemplos abaixo o tópico é `pedidos.integracao.envio`.

## Confirmar o tópico

```text
listar_topicos(busca="pedidos")
obter_topico(topico="pedidos.integracao.envio")
```

## Achar a mensagem pelo ProcessoID

Igual ao menu Topics → Messages, na janela recente:

```text
buscar_mensagens(
  topico="pedidos.integracao.envio",
  processo_id="COLOQUE-O-PROCESSO",
  limite=20,
  origem="recentes")
```

Se `encontradas=0`:

```text
buscar_mensagens(
  topico="pedidos.integracao.envio",
  processo_id="COLOQUE-O-PROCESSO",
  limite=50,
  origem="inicio")
```

`texto` substitui o ProcessoID quando a busca não é por esse campo. Se os dois
vierem preenchidos, vale `processo_id`.

## O que a resposta traz

- cluster, tópico, `seekType` / `seekDirection`
- `varredura_kafka_ui` (quantas mensagens o portal leu até filtrar)
- cada mensagem: partition, offset, timestamp, key e o JSON do conteúdo

Não há tool para publicar nem apagar.

# Publicação NuGet

| Campo | Valor |
| --- | --- |
| **PackageId** | `McpKafka` |
| **Tipo** | .NET Global Tool (`DotnetTool`) |
| **Comando CLI** | `mcp-kafka` |
| **Target** | `net8.0` |
| **Conta NuGet** | `matneves` |
| **GitHub** | `SouzaMatheus-dev/mcp-kafka` |

## Consumir

```powershell
dotnet tool install --global McpKafka
dotnet tool update --global McpKafka
```

## Primeira publicação

O pacote ainda não existe no NuGet.org. Antes da tag `v0.1.0`, cadastre o Trusted Publishing:

1. Entre em https://www.nuget.org/account/publishers com o usuário `matneves`.
2. Em **Trusted Publishing**, adicione uma política de GitHub Actions.
3. Preencha:

| Campo | Valor |
| --- | --- |
| Proprietário do GitHub | `SouzaMatheus-dev` |
| Repositório | `mcp-kafka` |
| Workflow | `.github/workflows/publish-nuget.yml` |
| Usuário NuGet | `matneves` |

4. Confirme a política. O workflow já pede `id-token: write` e usa `NuGet/login` com `user: matneves`.
5. Envie a tag:

```powershell
git tag v0.1.0
git push origin v0.1.0
```

O workflow testa, empacota, publica `McpKafka` 0.1.0 e abre a release no GitHub. Não é preciso API key no repositório.

## Versões seguintes

Atualize `<Version>` em `dotnet/McpKafka/McpKafka.csproj` e envie a tag correspondente (`v0.1.1`, `v0.2.0`, …):

```powershell
git tag v0.1.1
git push origin v0.1.1
```

## Pacote local

Serve para instalar na máquina antes do NuGet.org. Não substitui o Trusted Publishing.

```powershell
dotnet test dotnet/McpKafka.slnx
dotnet pack dotnet/McpKafka/McpKafka.csproj -c Release -o dotnet/nupkg
dotnet tool install --global --add-source dotnet/nupkg McpKafka
```

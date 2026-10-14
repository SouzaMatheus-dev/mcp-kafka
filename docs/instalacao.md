# Instalação

## Global tool (.NET 8+)

```powershell
dotnet tool install --global McpKafka
dotnet tool update --global McpKafka
mcp-kafka
```

`mcp-kafka` espera stdio MCP. Ctrl+C encerra.

Se o comando não for encontrado, feche o terminal e abra outro (PATH).

## A partir do código

```powershell
dotnet test dotnet/McpKafka.slnx
dotnet pack dotnet/McpKafka/McpKafka.csproj -c Release -o dotnet/nupkg
dotnet tool install --global --add-source dotnet/nupkg McpKafka
```

## dotnet dnx (.NET 10+)

```powershell
dotnet dnx McpKafka --yes --source https://api.nuget.org/v3/index.json
```

Em máquinas com .NET 8/9, use a global tool.

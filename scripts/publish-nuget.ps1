param(
    [string] $Configuration = "Release"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$project = Join-Path $root "dotnet\McpKafka\McpKafka.csproj"
$outDir = Join-Path $root "dotnet\nupkg"

dotnet test (Join-Path $root "dotnet\McpKafka.slnx") -c $Configuration
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

dotnet pack $project -c $Configuration -o $outDir
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$package = Get-ChildItem -Path (Join-Path $outDir "*.nupkg") | Select-Object -First 1
if (-not $package) { throw "Nenhum .nupkg gerado." }

if (-not $env:NUGET_API_KEY) {
    Write-Host "Pacote gerado: $($package.FullName)"
    Write-Host "Defina NUGET_API_KEY para publicar, ou use a tag v* no GitHub Actions."
    exit 0
}

dotnet nuget push $package.FullName `
    --api-key $env:NUGET_API_KEY `
    --source https://api.nuget.org/v3/index.json

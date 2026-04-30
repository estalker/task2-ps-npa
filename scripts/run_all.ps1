param(
  [string]$Model = "qwen2.5:7b-instruct",
  [string]$InputDir = "input",
  [string]$DocSource = "profstandard"
)

$ErrorActionPreference = "Stop"

function Assert-DockerEngineRunning {
  try {
    docker info *>$null
  } catch {
    throw "Docker engine is not reachable. Start Docker Desktop, wait until it says 'Running', then re-run this script."
  }
}

Write-Host "Starting Neo4j (docker compose)..."
Assert-DockerEngineRunning
docker compose up -d

Write-Host "Setting env vars for local Ollama + Neo4j (user scope)..."
powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "set_env.ps1") -Model $Model

Write-Host ""
Write-Host "Next steps:"
Write-Host "1) In a separate terminal, run Ollama server:"
Write-Host ("   powershell -ExecutionPolicy Bypass -File scripts\run_ollama.ps1 -Model `"{0}`"" -f $Model)
Write-Host "2) Put .docx into input folder, then ingest:"
Write-Host ("   python -m app.ingest --input {0} --doc-source `"{1}`"" -f $InputDir, $DocSource)


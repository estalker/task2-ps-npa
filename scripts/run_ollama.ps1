param(
  [string]$Model = "qwen2.5:7b-instruct"
)

$ErrorActionPreference = "Stop"

function Resolve-OllamaExe {
  $cmd = Get-Command "ollama" -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  $candidates = @(
    (Join-Path $env:LOCALAPPDATA "Programs\Ollama\ollama.exe"),
    (Join-Path $env:ProgramFiles "Ollama\ollama.exe")
  )

  foreach ($p in $candidates) {
    if (Test-Path $p) { return $p }
  }

  throw "ollama.exe not found. Install Ollama: https://ollama.com/download"
}

$OllamaExe = Resolve-OllamaExe

Write-Host "Pulling model (if missing): $Model"
& $OllamaExe pull $Model

Write-Host "Starting Ollama server (Ctrl+C to stop)..."
& $OllamaExe serve


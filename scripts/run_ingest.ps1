param(
  [string]$InputDir = "input",
  [string]$DocSource = "profstandard",
  [string]$BaseUrl = "http://localhost:11434/v1",
  [string]$ApiKey = "local",
  [string]$Model = "qwen2.5:3b-instruct",
  [int]$MaxChars = 1000,
  [int]$MaxChunks = 12,
  [int]$TimeoutS = 240
)

$ErrorActionPreference = "Stop"

$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"

$env:OPENAI_BASE_URL = $BaseUrl
$env:OPENAI_API_KEY = $ApiKey
$env:OPENAI_MODEL = $Model
$env:OLLAMA_TIMEOUT_S = "$TimeoutS"

python -u -m app.ingest --input $InputDir --doc-source $DocSource --llm-max-chars $MaxChars --llm-max-chunks $MaxChunks


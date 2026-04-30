param(
  [string]$BaseUrl = "http://localhost:11434/v1",
  [string]$ApiKey = "local",
  [string]$Model = "qwen2.5:7b-instruct",
  [string]$Neo4jUri = "neo4j://localhost:7687",
  [string]$Neo4jUser = "neo4j",
  [string]$Neo4jPassword = "neo4j_password"
)

$ErrorActionPreference = "Stop"

Write-Host "Setting user environment variables (setx). Re-open terminal after this."

setx OPENAI_BASE_URL $BaseUrl | Out-Null
setx OPENAI_API_KEY $ApiKey | Out-Null
setx OPENAI_MODEL $Model | Out-Null

setx NEO4J_URI $Neo4jUri | Out-Null
setx NEO4J_USER $Neo4jUser | Out-Null
setx NEO4J_PASSWORD $Neo4jPassword | Out-Null

Write-Host "Done."


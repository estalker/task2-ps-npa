param(
  [Parameter(Mandatory=$true)]
  [string]$BundleDir,

  [string]$ContainerName = "task2_ollama",
  [int]$Port = 11434,

  # Optional. If empty, we try volume_name.txt, else default to "ollama_data".
  [string]$VolumeName = "",

  # Image tag to run after restore (must exist after docker load).
  [string]$RunImage = "ollama/ollama:latest"
)

$ErrorActionPreference = "Stop"

function Require-File([string]$Path) {
  if (!(Test-Path $Path)) { throw "Not found: $Path" }
}

$bundlePath = (Resolve-Path $BundleDir).Path
$imgTar = Join-Path $bundlePath "ollama-image.tar"
$volTgz = Join-Path $bundlePath "ollama-volume.tgz"
$volNameFile = Join-Path $bundlePath "volume_name.txt"

Require-File $imgTar
Require-File $volTgz

if ([string]::IsNullOrWhiteSpace($VolumeName)) {
  if (Test-Path $volNameFile) {
    $VolumeName = (Get-Content $volNameFile -Raw).Trim()
  }
  if ([string]::IsNullOrWhiteSpace($VolumeName)) {
    $VolumeName = "ollama_data"
  }
}

Write-Host "==> BundleDir: $bundlePath"
Write-Host "==> Image tar:  $imgTar"
Write-Host "==> Volume tgz: $volTgz"
Write-Host "==> Volume:     $VolumeName"
Write-Host "==> Container:  $ContainerName"
Write-Host "==> Port:       $Port"
Write-Host "==> Run image:  $RunImage"

Write-Host "==> Loading image..."
docker load -i "$imgTar" | Write-Host

Write-Host "==> Creating volume (if missing)..."
docker volume create "$VolumeName" | Out-Null

Write-Host "==> Restoring volume contents..."
# Mount the bundle directory directly as /backup so paths are stable.
docker run --rm `
  -v "${VolumeName}:/v" `
  -v "${bundlePath}:/backup" `
  alpine:3.20 sh -c "cd /v && tar -xzf /backup/ollama-volume.tgz"

Write-Host "==> Restarting Ollama container..."
if (docker ps -a --format "{{.Names}}" | Select-String -SimpleMatch -Quiet $ContainerName) {
  docker rm -f "$ContainerName" | Out-Null
}

docker run -d --name "$ContainerName" `
  -p "${Port}:11434" `
  -v "${VolumeName}:/root/.ollama" `
  "$RunImage" | Out-Null

Write-Host "==> DONE. Check models:"
Write-Host "   curl http://localhost:$Port/api/tags"
Write-Host "   docker logs -f $ContainerName"


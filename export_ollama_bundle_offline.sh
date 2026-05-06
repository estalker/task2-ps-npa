#!/usr/bin/env bash
set -euo pipefail

# Offline export of:
# - Ollama docker image (as used by an existing container)
# - Ollama models volume mounted at /root/.ollama in that container
#
# No network access is required. This script DOES NOT run `docker pull`.
#
# Usage:
#   chmod +x export_ollama_bundle_offline.sh
#   ./export_ollama_bundle_offline.sh [out_dir]
#
# Optional env:
#   OLLAMA_CONTAINER=task2_ollama
#   HELPER_IMAGE=<local image with sh+tar>   (fallback if Ollama image lacks tar)

OUT_DIR="${1:-./ollama_bundle}"
CONTAINER_NAME="${OLLAMA_CONTAINER:-task2_ollama}"
HELPER_IMAGE="${HELPER_IMAGE:-}"

mkdir -p "$OUT_DIR"

echo "==> Export dir:  $OUT_DIR"
echo "==> Container:   $CONTAINER_NAME"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found in PATH" >&2
  exit 1
fi

if ! docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "ERROR: container '$CONTAINER_NAME' not found." >&2
  echo "Hint: docker ps --format 'table {{.Names}}\t{{.Image}}'" >&2
  exit 1
fi

# Detect image reference used by the container (already present locally)
IMAGE_REF="$(docker inspect "$CONTAINER_NAME" --format '{{.Config.Image}}')"
IMAGE_ID="$(docker inspect "$CONTAINER_NAME" --format '{{.Image}}')"
echo "==> Image ref:   $IMAGE_REF"
echo "==> Image id:    $IMAGE_ID"

# Detect volume mounted at /root/.ollama
VOL_NAME="$(
  docker inspect "$CONTAINER_NAME" \
    --format '{{range .Mounts}}{{if and (eq .Type "volume") (eq .Destination "/root/.ollama")}}{{.Name}}{{end}}{{end}}'
)"
if [[ -z "${VOL_NAME}" ]]; then
  echo "ERROR: cannot detect volume mounted to /root/.ollama in container '$CONTAINER_NAME'." >&2
  echo "Hint: docker inspect $CONTAINER_NAME | less" >&2
  exit 1
fi
echo "==> Volume:      $VOL_NAME"

echo "==> Saving Ollama image (no pull)"
docker save -o "$OUT_DIR/ollama-image.tar" "$IMAGE_ID"
echo "==> Wrote:       $OUT_DIR/ollama-image.tar"

echo "==> Archiving volume (models) -> $OUT_DIR/ollama-volume.tgz"

pick_helper() {
  # 1) Prefer explicit HELPER_IMAGE if provided
  if [[ -n "${HELPER_IMAGE}" ]]; then
    echo "$HELPER_IMAGE"
    return 0
  fi
  # 2) Try to use the same image as the container, if it has tar
  if docker run --rm --entrypoint sh "$IMAGE_ID" -c "command -v tar >/dev/null 2>&1" >/dev/null 2>&1; then
    echo "$IMAGE_ID"
    return 0
  fi
  return 1
}

if HELPER="$(pick_helper)"; then
  echo "==> Helper image: $HELPER"
else
  echo "ERROR: could not find a local helper image with 'sh' + 'tar'." >&2
  echo "Your Ollama image does not seem to contain 'tar'." >&2
  echo "" >&2
  echo "Fix options (offline):" >&2
  echo "  - If you have any local image with tar (e.g. busybox/alpine/ubuntu), set:" >&2
  echo "      HELPER_IMAGE=busybox:latest ./export_ollama_bundle_offline.sh" >&2
  echo "  - Or export that helper image from another machine and docker load it here." >&2
  exit 1
fi

docker run --rm \
  -v "${VOL_NAME}:/v:ro" \
  -v "$(pwd)/${OUT_DIR}:/backup" \
  --entrypoint sh \
  "$HELPER" -c "cd /v && tar -czf /backup/ollama-volume.tgz ."

echo "$VOL_NAME" > "$OUT_DIR/volume_name.txt"
echo "==> Wrote:       $OUT_DIR/ollama-volume.tgz"
echo "==> Wrote:       $OUT_DIR/volume_name.txt"
echo "==> DONE. Copy '$OUT_DIR' to Windows."


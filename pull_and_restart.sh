#!/usr/bin/env bash
# Запуск с рабочего стола / из файлового менеджера RedOS:
#   chmod +x pull_and_restart.sh
#   ПКМ → «Открыть в терминале» / «Выполнить» (зависит от оболочки).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
cd "$ROOT"

echo "==> Каталог: $ROOT"
echo "==> git pull"
git pull

echo "==> Сборка frontend и перезапуск контейнеров"
if command -v docker >/dev/null 2>&1; then
  docker compose up -d --build --force-recreate
elif command -v podman >/dev/null 2>&1; then
  podman compose up -d --build --force-recreate
else
  echo "Ошибка: не найдены docker и podman." >&2
  exit 1
fi

echo "==> Готово."
if [ -t 0 ] && [ -t 1 ]; then
  read -r -p "Нажмите Enter для выхода... " _ || true
fi

@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

echo ==^> Каталог: %CD%
echo ==^> git pull
git pull
if errorlevel 1 (
  echo Ошибка git pull.
  pause
  exit /b 1
)

echo ==^> docker compose: сборка и перезапуск
docker compose up -d --build --force-recreate
if errorlevel 1 (
  echo Ошибка docker compose. Убедитесь, что Docker Desktop запущен.
  pause
  exit /b 1
)

echo ==^> Готово.
pause

@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set COMPOSE_FILE=docker-compose.ollama.yml

echo ==^> Каталог: %CD%

if not exist "%COMPOSE_FILE%" (
  echo Ошибка: не найден %COMPOSE_FILE% рядом со скриптом.
  echo Ожидалось: %CD%\%COMPOSE_FILE%
  pause
  exit /b 1
)

echo ==^> git pull
git pull
if errorlevel 1 (
  echo Ошибка git pull.
  pause
  exit /b 1
)

echo ==^> docker compose pull (ollama image)
docker compose -f "%COMPOSE_FILE%" pull ollama
if errorlevel 1 (
  echo Ошибка docker compose pull. Убедитесь, что Docker Desktop запущен.
  pause
  exit /b 1
)

echo ==^> docker compose up -d (restart ollama, keep volume with models)
docker compose -f "%COMPOSE_FILE%" up -d --force-recreate ollama
if errorlevel 1 (
  echo Ошибка docker compose up. Убедитесь, что Docker Desktop запущен.
  pause
  exit /b 1
)

echo ==^> Готово. Container: task2_ollama, volume: ollama_data
echo ==^> Проверка: docker logs -f task2_ollama
pause


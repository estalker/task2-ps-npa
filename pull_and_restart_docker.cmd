@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set COMPOSE_MAIN=docker-compose.yml
set COMPOSE_OLLAMA=docker-compose.ollama.yml

echo ==^> Каталог: %CD%

if not exist "%COMPOSE_MAIN%" (
  echo Ошибка: не найден %COMPOSE_MAIN% рядом со скриптом.
  echo Ожидалось: %CD%\%COMPOSE_MAIN%
  pause
  exit /b 1
)

if not exist "%COMPOSE_OLLAMA%" (
  echo Ошибка: не найден %COMPOSE_OLLAMA% рядом со скриптом.
  echo Ожидалось: %CD%\%COMPOSE_OLLAMA%
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

echo ==^> docker compose pull (neo4j, ollama)
docker compose -f "%COMPOSE_MAIN%" -f "%COMPOSE_OLLAMA%" pull neo4j ollama
if errorlevel 1 (
  echo Ошибка docker compose pull. Убедитесь, что Docker Desktop запущен.
  pause
  exit /b 1
)

echo ==^> cleanup: stop/remove existing containers with fixed names
REM container_name is fixed in compose, so we must clear conflicts across projects
docker rm -f task2_frontend >nul 2>nul
docker rm -f task2_neo4j >nul 2>nul
docker rm -f task2_ollama >nul 2>nul

echo ==^> docker compose down (keep volumes)
docker compose -f "%COMPOSE_MAIN%" -f "%COMPOSE_OLLAMA%" down --remove-orphans
REM ignore errors from down (e.g. first run)

echo ==^> docker compose up -d --build (recreate all containers)
docker compose -f "%COMPOSE_MAIN%" -f "%COMPOSE_OLLAMA%" up -d --build --force-recreate --remove-orphans
if errorlevel 1 (
  echo Ошибка docker compose up. Убедитесь, что Docker Desktop запущен.
  pause
  exit /b 1
)

echo ==^> Готово. Containers: task2_frontend, task2_neo4j, task2_ollama
echo ==^> Проверка:
echo     docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo     docker logs -f task2_frontend
echo     docker logs -f task2_ollama
pause


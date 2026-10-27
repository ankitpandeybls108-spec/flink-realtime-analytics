@echo off
setlocal
cd /d "%~dp0"
docker compose down
if errorlevel 1 (
  echo Could not stop containers. Make sure Docker Desktop is running.
  pause
  exit /b 1
)
echo Containers stopped.
pause

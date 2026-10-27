@echo off
setlocal
cd /d "%~dp0"

echo =====================================================
echo Order event producer
echo =====================================================

where py >nul 2>&1
if not errorlevel 1 (
  set "PYTHON=py"
) else (
  where python >nul 2>&1
  if errorlevel 1 (
    echo ERROR: Python was not found. Install Python 3 from https://www.python.org/downloads/windows/
    pause
    exit /b 1
  )
  set "PYTHON=python"
)

%PYTHON% -c "import pandas, openpyxl, kafka" >nul 2>&1
if errorlevel 1 (
  echo Installing producer dependencies...
  %PYTHON% -m pip install -r producer\requirements.txt
  if errorlevel 1 (
    echo ERROR: Could not install Python dependencies.
    pause
    exit /b 1
  )
)

echo Sending up to 10000 rows from data\Online Retail.xlsx.
echo Press Ctrl+C to stop early.
%PYTHON% producer\producer.py --file "data\Online Retail.xlsx" --limit 10000 --delay-ms 20
if errorlevel 1 (
  echo ERROR: Producer stopped with an error. Confirm start-project.cmd has completed and Kafka is running.
  pause
  exit /b 1
)

echo Producer finished.
pause

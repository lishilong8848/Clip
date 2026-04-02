@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo           ClipFlow starting...
echo ========================================
echo.

set "PYTHON=%~dp0bin\.venv\Scripts\python.exe"
set "MAIN_EXE=%~dp0bin\ClipFlow.exe"
set "MAIN_PY=%~dp0bin\refactored_main.py"

if exist "%MAIN_EXE%" (
  "%MAIN_EXE%"
) else (
  if exist "%PYTHON%" (
    "%PYTHON%" "%MAIN_PY%"
  ) else (
    python "%MAIN_PY%"
  )
)

echo.
echo ========================================
echo           ClipFlow exited
echo ========================================
echo If there is an error, please send the screenshot to support.
echo.
pause

@echo off
title Slovo - Local Server
color 0B
cd /d "%~dp0"

echo.
echo  =========================================
echo   SLOVO - starting local server...
echo  =========================================
echo.

REM Check Node is installed
where node >nul 2>nul
if errorlevel 1 (
  echo  ERROR: Node.js is not installed.
  echo.
  echo  Download and install Node from:
  echo    https://nodejs.org
  echo.
  echo  Pick the LTS version, restart your computer, then run this again.
  echo.
  pause
  exit /b 1
)

REM Install dependencies if needed (only first time)
if not exist "node_modules" (
  echo  First run: installing dependencies. This takes about 20 seconds...
  echo.
  call npm install
  if errorlevel 1 (
    echo.
    echo  npm install failed. Check your internet connection and try again.
    pause
    exit /b 1
  )
  echo.
)

REM Open browser to the game after a short delay so server has time to boot
start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:3000"

echo  Server starting on http://localhost:3000
echo  Browser will open automatically.
echo.
echo  To stop the server: close this window.
echo.

node server.js

pause

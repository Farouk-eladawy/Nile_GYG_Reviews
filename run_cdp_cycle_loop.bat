@echo off
title GYG Airtable Sync (CDP Per-Cycle)

setlocal EnableExtensions
set "ScriptDir=%~dp0"
cd /d "%ScriptDir%"

set "PF86=%ProgramFiles(x86)%"

echo Starting Sync Service (CDP per cycle)...
echo Logs will be saved to sync_log.txt and displayed here.

set "QUIET=1"
if /I "%~1"=="--debug" (
    set "QUIET=0"
    echo on
    shift
)
if "%~1"=="--check" set "QUIET=0"

set "PYTHON_EXE=python"
if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"

if not exist ".venv\Scripts\python.exe" (
    python -m venv .venv
    set "PYTHON_EXE=.venv\Scripts\python.exe"
)

if "%QUIET%"=="1" (
    "%PYTHON_EXE%" -m pip install -r requirements.txt > nul 2>&1
) else (
    "%PYTHON_EXE%" -m pip install -r requirements.txt
)
if %ERRORLEVEL% NEQ 0 (
    echo Error installing dependencies.
    exit /b 1
)

if "%QUIET%"=="1" (
    "%PYTHON_EXE%" -m playwright install chromium > nul 2>&1
) else (
    "%PYTHON_EXE%" -m playwright install chromium
)
if %ERRORLEVEL% NEQ 0 (
    echo Error installing Playwright browsers.
    exit /b 1
)

set "PORT=9222"
set "BROWSER_TYPE="
set "CDP_URL="
set "CYCLE_DELAY_SECONDS=60"

if exist "%ScriptDir%.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%ScriptDir%.env") do (
        if /I "%%A"=="BROWSER_TYPE" set "BROWSER_TYPE=%%B"
        if /I "%%A"=="CDP_URL" set "CDP_URL=%%B"
        if /I "%%A"=="CYCLE_DELAY_SECONDS" set "CYCLE_DELAY_SECONDS=%%B"
    )
)

if "%BROWSER_TYPE%"=="" set "BROWSER_TYPE=edge"

if not "%~2"=="" (
    set "PORT=%~2"
) else (
    if defined CDP_URL (
        for /f "tokens=3 delims=:" %%P in ("%CDP_URL%") do set "PORT=%%P"
    )
)

set "BROWSER_PATH="

if /I "%BROWSER_TYPE%"=="chrome" (
    if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
    if not defined BROWSER_PATH if exist "%PF86%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%PF86%\Google\Chrome\Application\chrome.exe"
    if not defined BROWSER_PATH if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%LocalAppData%\Google\Chrome\Application\chrome.exe"
)

if /I "%BROWSER_TYPE%"=="edge" (
    if exist "%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
    if not defined BROWSER_PATH if exist "%PF86%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%PF86%\Microsoft\Edge\Application\msedge.exe"
    if not defined BROWSER_PATH if exist "%LocalAppData%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%LocalAppData%\Microsoft\Edge\Application\msedge.exe"
)

if not defined BROWSER_PATH (
    if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
    if not defined BROWSER_PATH if exist "%PF86%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%PF86%\Google\Chrome\Application\chrome.exe"
    if not defined BROWSER_PATH if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "BROWSER_PATH=%LocalAppData%\Google\Chrome\Application\chrome.exe"
    if not defined BROWSER_PATH if exist "%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
    if not defined BROWSER_PATH if exist "%PF86%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%PF86%\Microsoft\Edge\Application\msedge.exe"
    if not defined BROWSER_PATH if exist "%LocalAppData%\Microsoft\Edge\Application\msedge.exe" set "BROWSER_PATH=%LocalAppData%\Microsoft\Edge\Application\msedge.exe"
)

if not defined BROWSER_PATH (
    echo Selected browser type was not found: %BROWSER_TYPE%
    echo Checked Program Files and LocalAppData locations.
    pause
    exit /b 1
)

if "%~1"=="--check" (
    echo Using Python: %PYTHON_EXE%
    echo Using Browser: "%BROWSER_PATH%"
    echo Using Port: %PORT%
    set "BROWSER_ARGS=--remote-debugging-port=%PORT% --user-data-dir=%TEMP%\GYG-CDP-Profile"
    call :start_browser
    if not defined BROWSER_PID (
        echo Failed to start browser.
        exit /b 1
    )
    set "BROWSER_MODE=cdp"
    set "CDP_URL=http://127.0.0.1:%PORT%"
    timeout /t 2 > nul
    "%PYTHON_EXE%" -c "from playwright.sync_api import sync_playwright; p=sync_playwright().start(); b=p.chromium.connect_over_cdp('http://127.0.0.1:%PORT%'); (b.contexts[0] if b.contexts else b.new_context()).new_page(); b.close(); p.stop(); print('cdp_ok')"
    set "CHECK_EXIT=%ERRORLEVEL%"
    call :stop_browser
    exit /b %CHECK_EXIT%
)

if "%~1"=="--once" (
    call :run_once
    exit /b %ERRORLEVEL%
)

:loop
call :run_once
if %ERRORLEVEL% NEQ 0 (
    echo Script crashed. Restarting in 10 seconds...
    timeout /t 10 > nul
)
timeout /t %CYCLE_DELAY_SECONDS% > nul
goto loop

:run_once
set "BROWSER_ARGS=--remote-debugging-port=%PORT% --user-data-dir=%TEMP%\GYG-CDP-Profile"
call :start_browser
if not defined BROWSER_PID (
    echo Failed to start browser.
    exit /b 1
)

set "BROWSER_MODE=cdp"
set "CDP_URL=http://127.0.0.1:%PORT%"

timeout /t 2 > nul
"%PYTHON_EXE%" gyg_sync.py --once
set "SCRIPT_EXIT=%ERRORLEVEL%"
call :stop_browser
exit /b %SCRIPT_EXIT%

:start_browser
set "BROWSER_PID="
set "BROWSER_PATH=%BROWSER_PATH%"
for /f %%P in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "$p = Start-Process -FilePath $env:BROWSER_PATH -ArgumentList $env:BROWSER_ARGS -PassThru; $p.Id"') do set "BROWSER_PID=%%P"
exit /b 0

:stop_browser
if defined BROWSER_PID (
    taskkill /PID %BROWSER_PID% /T /F > nul 2>&1
    set "BROWSER_PID="
)
exit /b 0

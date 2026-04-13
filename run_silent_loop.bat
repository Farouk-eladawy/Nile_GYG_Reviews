@echo off
title GYG Airtable Sync (Silent Mode)
echo Starting Sync Service...
echo Logs will be saved to sync_log.txt and displayed here.
echo Browser visibility is controlled by .env file (HEADLESS_MODE).

setlocal EnableDelayedExpansion
set "ScriptDir=%~dp0"
cd /d "%ScriptDir%"

set "QUIET=1"
if "%~1"=="--check" set "QUIET=0"

set "BROWSER_MODE=launch"
set "BROWSER_TYPE=chromium"
set "HEADLESS_MODE=false"
set "CDP_URL=http://127.0.0.1:9222"
set "ACCOUNT_COUNT=1"
set "PROFILES_DIR=.profiles"
set "BROWSER_PORT_BASE=9300"
if exist ".env" (
    for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
        if /I "%%A"=="BROWSER_MODE" set "BROWSER_MODE=%%B"
        if /I "%%A"=="BROWSER_TYPE" set "BROWSER_TYPE=%%B"
        if /I "%%A"=="HEADLESS_MODE" set "HEADLESS_MODE=%%B"
        if /I "%%A"=="CDP_URL" set "CDP_URL=%%B"
        if /I "%%A"=="ACCOUNT_COUNT" set "ACCOUNT_COUNT=%%B"
        if /I "%%A"=="PROFILES_DIR" set "PROFILES_DIR=%%B"
        if /I "%%A"=="BROWSER_PORT_BASE" set "BROWSER_PORT_BASE=%%B"
    )
)

set "EFFECTIVE_MODE=%BROWSER_MODE%"
if /I "%EFFECTIVE_MODE%"=="true" set "EFFECTIVE_MODE=headless"
if /I "%EFFECTIVE_MODE%"=="false" set "EFFECTIVE_MODE=launch"
if /I "%EFFECTIVE_MODE%"=="background" set "EFFECTIVE_MODE=headless"
if /I "%EFFECTIVE_MODE%"=="hidden" set "EFFECTIVE_MODE=headless"
if /I "%HEADLESS_MODE%"=="true" set "EFFECTIVE_MODE=headless"
if /I "%HEADLESS_MODE%"=="headless" set "EFFECTIVE_MODE=headless"
if /I "%HEADLESS_MODE%"=="hidden" set "EFFECTIVE_MODE=headless"
if /I "%HEADLESS_MODE%"=="background" set "EFFECTIVE_MODE=headless"

set "CDP_PORT=9222"
for /f "tokens=4 delims=:/ " %%P in ("%CDP_URL%") do set "CDP_PORT=%%P"

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

if "%~1"=="--check" (
    echo Using Python: %PYTHON_EXE%
    "%PYTHON_EXE%" -c "from playwright.sync_api import sync_playwright; p=sync_playwright().start(); b=p.chromium.launch(headless=True); b.close(); p.stop(); print('browser_ok')"
    exit /b %ERRORLEVEL%
)

if "%~1"=="--once" (
    "%PYTHON_EXE%" gyg_sync.py --once
    exit /b %ERRORLEVEL%
)

if /I "%EFFECTIVE_MODE%"=="cdp" (
    if %ACCOUNT_COUNT% GTR 1 (
        echo Detected multi-account CDP mode. Launching %ACCOUNT_COUNT% browsers starting at port %BROWSER_PORT_BASE%...
        call ".\open_cdp_browsers_multi.bat" %BROWSER_PORT_BASE% %BROWSER_TYPE% %PROFILES_DIR% %ACCOUNT_COUNT%
    ) else (
        echo Detected visible CDP mode. Launching %BROWSER_TYPE% on port %CDP_PORT%...
        call ".\open_cdp_browser.bat" %CDP_PORT% "%PROFILES_DIR%\account_1\browser_user_data" %BROWSER_TYPE%
    )
    if %ERRORLEVEL% NEQ 0 (
        echo Failed to launch CDP browser.
        exit /b 1
    )
) else (
    echo Detected background mode. Running without opening a visible browser window.
)

:loop
"%PYTHON_EXE%" gyg_sync.py
if %ERRORLEVEL% NEQ 0 (
    echo Script crashed. Restarting in 10 seconds...
    timeout /t 10
    goto loop
)
pause

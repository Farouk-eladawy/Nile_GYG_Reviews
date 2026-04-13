@echo off
set PORT=9222
if not "%~1"=="" set PORT=%~1
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" (
    start "" "%ProgramFiles%\Google\Chrome\Application\chrome.exe" --remote-debugging-port=%PORT% --user-data-dir="%TEMP%\GYG-CDP-Profile"
    exit /b 0
)
if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" (
    start "" "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" --remote-debugging-port=%PORT% --user-data-dir="%TEMP%\GYG-CDP-Profile"
    exit /b 0
)
if exist "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" (
    start "" "%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=%PORT% --user-data-dir="%TEMP%\GYG-CDP-Profile"
    exit /b 0
)
if exist "%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" (
    start "" "%ProgramFiles%\Microsoft\Edge\Application\msedge.exe" --remote-debugging-port=%PORT% --user-data-dir="%TEMP%\GYG-CDP-Profile"
    exit /b 0
)
echo Chrome or Edge was not found.
exit /b 1

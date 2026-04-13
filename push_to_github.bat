@echo off
setlocal enabledelayedexpansion

:: Change to the directory where the batch file is located
cd /d "%~dp0"

:: Read .env file to get GITHUB_REPO_URL
set GITHUB_REPO_URL=
for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    if "%%A"=="GITHUB_REPO_URL" set GITHUB_REPO_URL=%%B
)

if "!GITHUB_REPO_URL!"=="" (
    echo [ERROR] GITHUB_REPO_URL is not set in .env file.
    echo Please add GITHUB_REPO_URL=https://github.com/USERNAME/REPOSITORY.git to your .env file.
    pause
    exit /b 1
)

echo Using Repository URL: !GITHUB_REPO_URL!

:: Check if git is initialized
if not exist ".git" (
    echo Initializing Git repository...
    git init
    git branch -M main
    git remote add origin "!GITHUB_REPO_URL!"
) else (
    :: Update remote url just in case it changed
    git remote set-url origin "!GITHUB_REPO_URL!"
)

echo Adding files to Git...
git add .

echo Committing changes...
set /p commit_msg="Enter commit message (or press enter for default 'Update from local environment'): "
if "!commit_msg!"=="" set commit_msg=Update from local environment
git commit -m "!commit_msg!"

echo Pushing to GitHub...
git push -u origin main

echo.
echo ==============================================
echo Done! Code pushed successfully.
echo ==============================================
pause
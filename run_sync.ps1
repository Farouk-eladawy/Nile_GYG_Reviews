<#
.SYNOPSIS
    Runs the GetYourGuide to Airtable Sync Script.
.DESCRIPTION
    Checks for Python, installs dependencies, and runs the sync script.
#>

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptDir

Write-Host "Checking environment..." -ForegroundColor Cyan

# Check Python
if (-not (Get-Command "python" -ErrorAction SilentlyContinue)) {
    Write-Host "Error: Python is not installed or not in PATH." -ForegroundColor Red
    Write-Host "Please install Python from https://www.python.org/downloads/"
    Read-Host "Press Enter to exit..."
    exit 1
}

# Install Requirements
Write-Host "Installing/Updating Python dependencies..." -ForegroundColor Cyan
try {
    python -m pip install -r requirements.txt
}
catch {
    Write-Host "Error installing dependencies." -ForegroundColor Red
    Read-Host "Press Enter to exit..."
    exit 1
}

# Install Playwright Browsers
Write-Host "Ensuring Playwright browsers are installed..." -ForegroundColor Cyan
python -m playwright install chromium

# Run the Script
Write-Host "Starting Sync Script..." -ForegroundColor Green
python gyg_sync.py

Write-Host "Script finished." -ForegroundColor Cyan
Read-Host "Press Enter to close..."

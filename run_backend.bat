@echo off
echo Starting Python backend for JOI...

REM Determine the script's own directory
set SCRIPT_DIR=%~dp0

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo CRITICAL: Python is not found in the system PATH.
    pause
    exit /b 1
)

REM Check if venv exists, if not create it in the script's directory
if not exist "%SCRIPT_DIR%venv" (
    echo Creating virtual environment...
    python -m venv "%SCRIPT_DIR%venv"
    if %errorlevel% neq 0 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
)

REM Activate virtual environment and install dependencies
echo Activating virtual environment...
call "%SCRIPT_DIR%venv\Scripts\activate.bat"

echo Installing/checking dependencies from requirements.txt...
pip install -r "%SCRIPT_DIR%requirements.txt" --log pip_install.log
if %errorlevel% neq 0 (
    echo Failed to install dependencies. Check pip_install.log for details.
    pause
    exit /b 1
)


REM Run the FastAPI server
echo Launching API server on port 8000...
python "%SCRIPT_DIR%api_server.py"
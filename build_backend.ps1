param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$backendRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvDir = Join-Path $backendRoot ".build-venv"
$distRoot = Join-Path $backendRoot "dist"
$distAppDir = Join-Path $distRoot "joi-backend"
$buildDir = Join-Path $backendRoot "build"
$specPath = Join-Path $backendRoot "joi-backend.spec"

Write-Host "== JOI Backend Build ==" -ForegroundColor Cyan
Write-Host "Backend root: $backendRoot"

if ($Clean) {
    Write-Host "Cleaning previous build artifacts..." -ForegroundColor Yellow
    if (Test-Path $distRoot) { Remove-Item -Recurse -Force $distRoot }
    if (Test-Path $buildDir) { Remove-Item -Recurse -Force $buildDir }
    if (Test-Path $specPath) { Remove-Item -Force $specPath }
}

if (-not (Test-Path $venvDir)) {
    Write-Host "Creating build virtualenv..." -ForegroundColor Yellow
    python -m venv $venvDir
}

$pythonExe = Join-Path $venvDir "Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found in build virtualenv: $pythonExe"
}

Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
& $pythonExe -m pip install --upgrade pip
& $pythonExe -m pip install -r (Join-Path $backendRoot "requirements.txt")
& $pythonExe -m pip install pyinstaller

Write-Host "Building backend executable via PyInstaller..." -ForegroundColor Yellow
Push-Location $backendRoot
try {
    & $pythonExe -m PyInstaller `
        --noconfirm `
        --clean `
        --onedir `
        --name "joi-backend" `
        --distpath $distRoot `
        --workpath $buildDir `
        --specpath $backendRoot `
        --collect-all openai `
        --collect-all googleapiclient `
        --collect-all google_auth_oauthlib `
        --collect-all google.auth `
        --collect-all google.generativeai `
        --collect-all uvicorn `
        --collect-all redis `
        --collect-all pymongo `
        --collect-all pyautogui `
        --collect-all psutil `
        --collect-all bs4 `
        --collect-all html2text `
        --hidden-import uvicorn.logging `
        --hidden-import uvicorn.loops.auto `
        --hidden-import uvicorn.protocols.http.auto `
        --hidden-import uvicorn.protocols.websockets.auto `
        api_server.py
}
finally {
    Pop-Location
}

if (-not (Test-Path $distAppDir)) {
    throw "Backend dist folder was not created: $distAppDir"
}

$envExampleSrc = Join-Path $backendRoot "dotenv-example"
if (Test-Path $envExampleSrc) {
    Copy-Item $envExampleSrc (Join-Path $distAppDir ".env.example") -Force
}

$googleCredsSrc = Join-Path $backendRoot "tools\credentials.json"
if (Test-Path $googleCredsSrc) {
    $toolsDistDir = Join-Path $distAppDir "tools"
    if (-not (Test-Path $toolsDistDir)) {
        New-Item -ItemType Directory -Path $toolsDistDir | Out-Null
    }
    Copy-Item $googleCredsSrc (Join-Path $toolsDistDir "credentials.json") -Force
}

$readmePath = Join-Path $distAppDir "README-BACKEND.txt"
@"
JOI Backend Runtime Package
===========================

1) Create a .env file in this folder (or use .env.example as reference).
2) Run joi-backend.exe to start API server on port 8000.
3) Ensure MongoDB and Redis are running and reachable from .env settings.

Notes:
- If Google Email/Calendar tools are used, credentials.json is expected in tools\.
- Runtime env file can be overridden via JOI_ENV_PATH environment variable.
"@ | Set-Content -Path $readmePath -Encoding UTF8

Write-Host ""
Write-Host "Backend build complete." -ForegroundColor Green
Write-Host "Output: $distAppDir"

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
        --collect-all faster_whisper `
        --collect-all ctranslate2 `
        --collect-all av `
        --collect-all tokenizers `
        --collect-all onnxruntime `
        --collect-all vosk `
        --collect-all sounddevice `
        --collect-all _sounddevice_data `
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

# Ship the secret-free template only. The developer's own .env and Google
# credentials.json hold real keys and must never go into an installer: anyone
# who downloads JOI could unpack them.
$templateSrc = Join-Path $backendRoot "packaged.env"
if (-not (Test-Path $templateSrc)) {
    throw "packaged.env template not found: $templateSrc"
}
Copy-Item $templateSrc (Join-Path $distAppDir "env-template.txt") -Force

$readmePath = Join-Path $distAppDir "README-BACKEND.txt"
@"
JOI Backend Runtime Package
===========================

The JOI desktop app starts this backend itself. On first run it copies
env-template.txt to the user's data folder as .env and generates a
per-install signing secret. Override the env file with JOI_ENV_PATH.
"@ | Set-Content -Path $readmePath -Encoding UTF8

# Last line of defence: refuse to finish if any secret-bearing file slipped in.
$forbidden = Get-ChildItem -Path $distAppDir -Recurse -Force -File | Where-Object {
    $_.Name -eq ".env" -or $_.Name -like ".env.*" -or $_.Name -eq "credentials.json" -or $_.Name -like "token*.pickle"
}
if ($forbidden) {
    $names = ($forbidden | ForEach-Object { $_.FullName }) -join "`n"
    throw "Secret files found in the backend package; refusing to continue:`n$names"
}

Write-Host ""
Write-Host "Backend build complete." -ForegroundColor Green
Write-Host "Output: $distAppDir"

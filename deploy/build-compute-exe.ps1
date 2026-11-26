param([string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$outputRoot = Join-Path $projectRoot 'outputs\compute-assistant'
if ($OutputDirectory) { $outputRoot = $OutputDirectory }
$buildRoot = Join-Path $projectRoot 'work\compute-exe-build'
New-Item -ItemType Directory -Force -Path $outputRoot,$buildRoot | Out-Null
python -m PyInstaller --noconfirm --onefile --windowed --name AI-Trader-Compute `
    --paths $PSScriptRoot --distpath $outputRoot --specpath $buildRoot `
    --workpath (Join-Path $buildRoot 'build') `
    --hidden-import aiohttp --hidden-import aiohttp._websocket `
    --exclude-module torch --exclude-module pandas --exclude-module matplotlib `
    --exclude-module pytest --exclude-module IPython `
    (Join-Path $PSScriptRoot 'compute_desktop.py')
if ($LASTEXITCODE -ne 0) { throw 'Compute EXE build failed' }
Get-Item (Join-Path $outputRoot 'AI-Trader-Compute.exe') | Select-Object FullName,Length

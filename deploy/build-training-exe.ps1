param([string]$OutputDirectory)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
$buildRoot = Join-Path $projectRoot 'work\training-exe-build'
$outputRoot = Join-Path $projectRoot 'outputs\training-assistant'
if ($OutputDirectory) { $outputRoot = $OutputDirectory }
New-Item -ItemType Directory -Path $buildRoot,$outputRoot -Force | Out-Null
python -m PyInstaller --noconfirm --clean --onefile --windowed `
    --name AI-Trader-Training `
    --paths $PSScriptRoot `
    --distpath $outputRoot --workpath (Join-Path $buildRoot 'build') --specpath $buildRoot `
    --exclude-module matplotlib --exclude-module pandas --exclude-module IPython `
    --exclude-module pytest --exclude-module torch --exclude-module tensorflow `
    --exclude-module cv2 --exclude-module notebook --exclude-module sphinx `
    (Join-Path $PSScriptRoot 'training_desktop.py')
if ($LASTEXITCODE -ne 0) { throw 'EXE build failed' }
Get-Item (Join-Path $outputRoot 'AI-Trader-Training.exe') | Select-Object FullName,Length,LastWriteTime

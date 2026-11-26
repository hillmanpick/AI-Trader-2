param([string]$ConfigPath = (Join-Path $PSScriptRoot '..\work\hybrid-training\config.json'))
$ErrorActionPreference = 'Stop'
$taskConfig = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$workerPath = Join-Path $PSScriptRoot 'local_training_worker.py'
$logPath = Join-Path $taskConfig.workspace 'worker.log'
$env:PYTHONUNBUFFERED = '1'
& $taskConfig.python -u $workerPath --config $ConfigPath *>> $logPath
exit $LASTEXITCODE

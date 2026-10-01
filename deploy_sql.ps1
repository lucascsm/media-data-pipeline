[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config/cloud.example.json'),
    [switch]$Apply,
    [string]$PythonExecutable = 'python'
)
$ErrorActionPreference = 'Stop'
$parameters = @((Join-Path $PSScriptRoot 'scripts/deploy_sql.py'), '--config', $ConfigPath)
if ($Apply) { $parameters += '--apply' }
& $PythonExecutable @parameters
if ($LASTEXITCODE -ne 0) { throw "SQL planner failed with exit code $LASTEXITCODE" }

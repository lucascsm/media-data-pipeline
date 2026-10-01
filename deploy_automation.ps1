[CmdletBinding()]
param(
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'config/cloud.example.json'),
    [ValidateSet('daily', 'reprocess', 'reach')][string]$RunMode = 'daily',
    [string]$StartDate,
    [string]$EndDate,
    [string[]]$Channels = @('google', 'meta', 'tiktok', 'kwai'),
    [switch]$InitializeResources,
    [switch]$PruneImages,
    [int]$KeepImages = 10,
    [switch]$Apply,
    [string]$PythonExecutable = 'python'
)
$ErrorActionPreference = 'Stop'
$parameters = @((Join-Path $PSScriptRoot 'scripts/deploy_job.py'), '--config', $ConfigPath, '--mode', $RunMode, '--channels', ($Channels -join ','))
if ($StartDate) { $parameters += @('--start', $StartDate) }
if ($EndDate) { $parameters += @('--end', $EndDate) }
if ($InitializeResources) { $parameters += '--initialize-resources' }
if ($PruneImages) { $parameters += @('--prune-images', '--keep-images', $KeepImages) }
if ($Apply) { $parameters += '--apply' }
& $PythonExecutable @parameters
if ($LASTEXITCODE -ne 0) { throw "Deploy planner failed with exit code $LASTEXITCODE" }

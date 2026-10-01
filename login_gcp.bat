@echo off
REM Explicit account and project arguments; no organization defaults.
if "%~1"=="" goto usage
if "%~2"=="" goto usage
gcloud auth login "%~1"
if errorlevel 1 exit /b 1
gcloud config set project "%~2"
exit /b %errorlevel%
:usage
echo Usage: login_gcp.bat YOUR_EMAIL YOUR_PROJECT_ID
exit /b 1

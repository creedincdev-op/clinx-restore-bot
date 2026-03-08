@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found in PATH.
  goto :end
)

if not exist ".env" (
  echo BOT_TOKEN=PASTE_YOUR_TOKEN_HERE>.env
  echo DEFAULT_BACKUP_GUILD_ID=>>.env
  echo [INFO] Created .env file.
  echo [ACTION] Open .env, paste token, then run start.bat again.
  goto :end
)

for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
  if /I "%%A"=="BOT_TOKEN" set "BOT_TOKEN=%%B"
  if /I "%%A"=="DEFAULT_BACKUP_GUILD_ID" set "DEFAULT_BACKUP_GUILD_ID=%%B"
)

if "%BOT_TOKEN%"=="" (
  echo [ERROR] BOT_TOKEN missing in .env
  goto :end
)

if /I "%BOT_TOKEN%"=="PASTE_YOUR_TOKEN_HERE" (
  echo [ERROR] Replace placeholder BOT_TOKEN in .env first.
  goto :end
)

echo Starting advanced slash bot...
python advanced_restore_bot.py

:end
echo.
pause

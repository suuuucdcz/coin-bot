@echo off
chcp 65001 >nul
set "FICHIER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\memecoin-radar.bat"
if exist "%FICHIER%" del "%FICHIER%"
echo Demarrage automatique du radar desactive.
pause

@echo off
chcp 65001 >nul
title Configuration - Memecoin Radar
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" goto config
echo Premiere installation : creation de l'environnement Python, une seule fois...
py -3 -m venv .venv
if not exist ".venv\Scripts\python.exe" python -m venv .venv
if not exist ".venv\Scripts\python.exe" goto nopython
".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto pipfail
:config
".venv\Scripts\python.exe" -m radar.setup
echo.
pause
exit /b 0
:nopython
echo Python introuvable. Installe Python 3.12 ou plus recent depuis python.org,
echo coche "Add python.exe to PATH" pendant l'installation, puis relance ce fichier.
pause
exit /b 1
:pipfail
echo L'installation des modules a echoue. Verifie ta connexion internet puis relance.
pause
exit /b 1

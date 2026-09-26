@echo off
chcp 65001 >nul
title Memecoin Radar
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" goto noinstall
if not exist ".env" goto noinstall
:boucle
echo [%date% %time%] Demarrage du radar...
".venv\Scripts\python.exe" -m radar.main
echo [%date% %time%] Le radar s'est arrete. Redemarrage dans 10 s - ferme la fenetre pour arreter.
timeout /t 10 /nobreak >nul
goto boucle
:noinstall
echo Le radar n'est pas encore configure : double-clique d'abord sur configurer.bat
pause
exit /b 1

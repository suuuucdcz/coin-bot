@echo off
chcp 65001 >nul
title Connexion X - Memecoin Radar
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" goto noinstall
echo Une fenetre Edge va s'ouvrir sur X.
echo 1. Connecte-toi - compte secondaire conseille.
echo 2. La fenetre se ferme toute seule une fois la connexion detectee.
echo.
".venv\Scripts\python.exe" -m radar.sources.x_watch login
echo.
echo Si le radar tourne deja, ferme-le et relance run.bat pour activer la veille X.
pause
exit /b 0
:noinstall
echo Double-clique d'abord sur configurer.bat
pause
exit /b 1

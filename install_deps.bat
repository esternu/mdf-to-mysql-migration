@echo off
echo ============================================
echo  MDF-to-MySQL Tool - Abhängigkeiten installieren
echo ============================================
echo.

py --version >nul 2>&1
if errorlevel 1 (
    echo FEHLER: Python nicht gefunden!
    echo Bitte Python 3.10+ von https://python.org installieren.
    pause
    exit /b 1
)

echo Installiere Python-Pakete ...
py -m pip install --upgrade pip
py -m pip install pyodbc mysql-connector-python

echo.
echo ============================================
echo  Fertig! Tool starten mit:
echo  start_tool.bat
echo ============================================
echo.
echo HINWEIS: Fuer .mdf-Dateien wird zusaetzlich
echo SQL Server LocalDB benoetigt:
echo https://aka.ms/sqllocaldb
echo (Kostenlos - ca. 50 MB)
echo.
pause

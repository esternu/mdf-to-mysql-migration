@echo off
:: MDF-to-MySQL Migration Tool starten

:: Sicherstellen, dass wir im richtigen Verzeichnis sind
cd /d "%~dp0"

:: Python-Version pruefen
py --version >nul 2>&1
if errorlevel 1 (
    echo FEHLER: Python nicht gefunden!
    echo Bitte Python 3.8+ von https://python.org installieren.
    echo Beim Installieren: Haken bei "Add Python to PATH" setzen!
    pause
    exit /b 1
)

:: Fehlende Pakete automatisch nachinstallieren
py -c "import pyodbc" >nul 2>&1
if errorlevel 1 (
    echo pyodbc fehlt - wird installiert ...
    py -m pip install pyodbc
)

py -c "import mysql.connector" >nul 2>&1
if errorlevel 1 (
    echo mysql-connector-python fehlt - wird installiert ...
    py -m pip install mysql-connector-python
)

:: Tool starten (kein Konsolenfenster im Hintergrund)
start "" py mdf_to_mysql.py

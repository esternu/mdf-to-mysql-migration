"""
Gemeinsame Pfad-Konstanten für alle src/-Module.
PROJECT_DIR zeigt auf das Verzeichnis, das src/ enthält (das Projekt-Root).
"""
import os
import datetime

_SRC_DIR    = os.path.dirname(os.path.abspath(__file__))   # …/mdf-to-mysql-migration/src
PROJECT_DIR = os.path.dirname(_SRC_DIR)                    # …/mdf-to-mysql-migration

CFG_FILE = os.path.join(PROJECT_DIR, "config.json")
LOG_DIR  = os.path.join(os.path.dirname(PROJECT_DIR), "mdf-to-mysql-logs")
TEMP_DIR = os.path.join(PROJECT_DIR, "temp")

os.makedirs(LOG_DIR,  exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Wird einmal beim Modulimport erzeugt – Zeitstempel des jeweiligen Programm-Starts.
LOG_FILE = os.path.join(
    LOG_DIR,
    f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

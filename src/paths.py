"""
Gemeinsame Pfad-Konstanten für alle src/-Module.
PROJECT_DIR zeigt auf das Verzeichnis, das src/ enthält (das Projekt-Root).
"""
import os
import datetime

_SRC_DIR    = os.path.dirname(os.path.abspath(__file__))   # …/mdf-to-mysql-migration/src
PROJECT_DIR = os.path.dirname(_SRC_DIR)                    # …/mdf-to-mysql-migration

CFG_FILE         = os.path.join(PROJECT_DIR, "config.json")
LOG_DIR          = os.path.join(os.path.dirname(PROJECT_DIR), "mdf-to-mysql-logs")
TEMP_DIR         = os.path.join(PROJECT_DIR, "temp")
CHECKPOINT_FILE  = os.path.join(TEMP_DIR, "migration_checkpoint.json")

# Kanonischer Schema-Ordner des WCEP-Projekts (Sibling-Repo). Schema-DDL und
# Audit-Trigger für die Cockpit_Datenbank werden hier zusätzlich gespiegelt,
# siehe WCEP/Tools/schema/README.md.
WCEP_SCHEMA_DIR  = os.path.join(os.path.dirname(PROJECT_DIR), "WCEP", "Tools", "schema")

# Kanonischer Dateiname des Schema-Dumps im WCEP-Repo (fest laut README-Contract,
# NICHT schema_<db>.sql). Nur für target_db == "Cockpit_Datenbank" relevant.
WCEP_SCHEMA_FILENAME = "Cockpit_DatenBank.sql"

os.makedirs(LOG_DIR,  exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Wird einmal beim Modulimport erzeugt – Zeitstempel des jeweiligen Programm-Starts.
LOG_FILE = os.path.join(
    LOG_DIR,
    f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

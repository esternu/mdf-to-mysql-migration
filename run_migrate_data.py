"""
Runner: SQL Server (MDF) → MySQL Datenmigration.
Liest Einstellungen aus config.json (erstes Profil),
baut Verbindungen auf und delegiert an src/migrate_data.py.
"""
import sys
import os
import io
import base64
import json
import datetime

# UTF-8 auf Windows-Konsole erzwingen
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_DIR, "src"))

from paths        import CFG_FILE, LOG_DIR
from mssql        import attach_mdf, detach_and_cleanup
from migrate_data import get_table_list, migrate_all

try:
    import mysql.connector
except ImportError:
    print("FEHLER: mysql-connector-python fehlt. Bitte: py -m pip install mysql-connector-python")
    sys.exit(1)

# ── Log ───────────────────────────────────────────────────────────────────────
_LOG_FILE = os.path.join(
    LOG_DIR,
    f"data_migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

def log(msg: str) -> None:
    ts   = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(_LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")

# ── Konfiguration ─────────────────────────────────────────────────────────────
with open(CFG_FILE, encoding="utf-8") as fh:
    all_cfg = json.load(fh)

profile    = next(iter(all_cfg))
cfg        = all_cfg[profile]
MDF_PATH   = cfg["mdf_path"]
DB_NAME    = cfg["db_attach_name"]
DRIVER     = cfg["driver"]
MYSQL_HOST = cfg["mysql_host"]
MYSQL_PORT = int(cfg["mysql_port"])
MYSQL_DB   = cfg["mysql_db"]
MYSQL_USER = "nocodb"
MYSQL_PASS = base64.b64decode(cfg["mysql_pass_b64"]).decode()

log(f"=== Datenmigration {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===")
log(f"Quelle:  {MDF_PATH}")
log(f"MySQL:   {MYSQL_HOST}:{MYSQL_PORT} / {MYSQL_DB}")
log("")

# ── SQL Server anhängen ───────────────────────────────────────────────────────
log("── Schritt 1: SQL Server MDF anhängen")
session = None
try:
    session = attach_mdf(MDF_PATH, DB_NAME, DRIVER, log)
except Exception as e:
    log(f"FEHLER: {e}")
    sys.exit(1)

# ── Tabellen ermitteln ────────────────────────────────────────────────────────
log("")
log("── Schritt 2: Tabellen ermitteln")
tables = get_table_list(session)
log(f"Gefunden: {len(tables)} Tabellen")
for s, t in tables:
    log(f"  - {s}.{t}")

# ── MySQL verbinden ───────────────────────────────────────────────────────────
log("")
log("── Schritt 3: MySQL verbinden")
try:
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, charset="utf8mb4",
        connection_timeout=10,
    )
    log("MySQL Verbindung erfolgreich.")
except Exception as e:
    log(f"FEHLER MySQL: {e}")
    detach_and_cleanup(session, log)
    sys.exit(1)

# ── Migrieren ─────────────────────────────────────────────────────────────────
log("")
log("── Schritt 4: Daten migrieren")
result = migrate_all(session, mysql_conn, tables, log)

# ── Cleanup ───────────────────────────────────────────────────────────────────
mysql_conn.close()
detach_and_cleanup(session, log)

# ── Zusammenfassung ───────────────────────────────────────────────────────────
log("")
log(f"=== Fertig: {result['total_rows']} Zeilen migriert ===")
if result["errors"]:
    log(f"⚠  {len(result['errors'])} Fehler:")
    for e in result["errors"]:
        log(f"   {e}")
else:
    log("✓ Keine Fehler.")
log(f"Log: {_LOG_FILE}")

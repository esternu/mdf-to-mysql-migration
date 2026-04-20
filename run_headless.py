"""
Headless-Runner für MDF-to-MySQL Migration.
Führt Schema lesen → DDL generieren → MySQL deployen ohne GUI aus.
Liest Einstellungen aus config.json (Profil 'Standard').
"""
import sys, os, json, base64, datetime, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Skript-Verzeichnis zum Pfad hinzufügen
_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)

# Log-Datei im gemeinsamen Log-Ordner anlegen
_LOG_DIR  = os.path.join(os.path.dirname(_DIR), "mdf-to-mysql-logs")
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(_LOG_FILE, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")

# Config laden
cfg_file = os.path.join(_DIR, "config.json")
with open(cfg_file, encoding="utf-8") as fh:
    all_cfg = json.load(fh)

profile = next(iter(all_cfg))
cfg     = all_cfg[profile]

MDF_PATH    = cfg["mdf_path"]
DB_NAME     = cfg["db_attach_name"]
DRIVER      = cfg["driver"]
MYSQL_HOST  = cfg["mysql_host"]
MYSQL_PORT  = int(cfg["mysql_port"])
MYSQL_USER  = cfg["mysql_user"]
MYSQL_PASS  = base64.b64decode(cfg["mysql_pass_b64"]).decode()
MYSQL_DB    = cfg["mysql_db"]

# Migrations-Modul laden
import mdf_to_mysql as m

log(f"=== Headless Migration Run  {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===")
log(f"Profil:    {profile}")
log(f"MDF:       {MDF_PATH}")
log(f"MySQL:     {MYSQL_HOST}:{MYSQL_PORT} / {MYSQL_DB}")
log("")

# ── 1) Schema lesen ───────────────────────────────────────────────────────
log("── Schritt 1: Schema lesen")
session = None
schema  = None
try:
    log("Original-Datei wird nicht verändert – Tool arbeitet auf Kopie.")
    session = m.attach_mdf(MDF_PATH, DB_NAME, DRIVER, log)
    schema  = m.read_schema(session, log)
except Exception as e:
    log(f"FEHLER beim Schema lesen: {e}")
    sys.exit(1)
finally:
    if session is not None:
        m.detach_and_cleanup(session, log)

# ── 2) DDL generieren ─────────────────────────────────────────────────────
log("")
log("── Schritt 2: DDL generieren")
try:
    ddl = m.generate_mysql_ddl(schema, MYSQL_DB)
    ddl_path = os.path.join(_DIR, "last_generated.sql")
    with open(ddl_path, "w", encoding="utf-8") as fh:
        fh.write(ddl)
    tcount = len(schema["tables"])
    vcount = len(schema["views"])
    log(f"DDL generiert: {tcount} Tabellen, {vcount} Views → {ddl_path}")
except Exception as e:
    log(f"FEHLER beim DDL generieren: {e}")
    sys.exit(1)

# ── 3) MySQL deployen ─────────────────────────────────────────────────────
log("")
log("── Schritt 3: MySQL deployen")
try:
    m.deploy_to_mysql(ddl, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB, log)
except Exception as e:
    log(f"FEHLER beim Deployen: {e}")
    sys.exit(1)

log("")
log(f"=== Run beendet. Log: {_LOG_FILE} ===")

"""
MDF to MySQL Migration Tool
Liest SQL Server .mdf Dateien und übertraegt Schema/Daten auf einen MySQL-Server (z.B. Synology).
Kompatibel mit Python 3.8+ auf Windows.
"""

# Windows: DPI-Bewusstsein aktivieren (scharfe Darstellung auf HiDPI-Monitoren)
try:
    from ctypes import windll
    windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import re
import os
import sys
import json
import base64
import datetime
import subprocess as _subprocess
from typing import Optional, List, Dict, Any

# Pfade
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))          # …/mdf-to-mysql-migration
_CFG_FILE   = os.path.join(_SCRIPT_DIR, "config.json")
_LOG_DIR    = os.path.join(os.path.dirname(_SCRIPT_DIR), "mdf-to-mysql-logs")  # eine Ebene höher
os.makedirs(_LOG_DIR, exist_ok=True)                               # Ordner beim Start anlegen
_LOG_FILE   = os.path.join(
    _LOG_DIR,
    f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

# ── Optionale Imports mit Fehlermeldung ──────────────────────────────────────
try:
    import pyodbc
    PYODBC_OK = True
except ImportError:
    PYODBC_OK = False

try:
    import mysql.connector
    MYSQL_OK = True
except ImportError:
    MYSQL_OK = False


# ════════════════════════════════════════════════════════════════════════════
#  SQL Server → MySQL Typ-Konvertierung
# ════════════════════════════════════════════════════════════════════════════
TYPE_MAP = {
    "nvarchar":       "VARCHAR",
    "nchar":          "CHAR",
    "ntext":          "LONGTEXT",
    "varchar":        "VARCHAR",
    "char":           "CHAR",
    "text":           "LONGTEXT",
    "int":            "INT",
    "bigint":         "BIGINT",
    "smallint":       "SMALLINT",
    "tinyint":        "TINYINT",
    "bit":            "TINYINT(1)",
    "decimal":        "DECIMAL",
    "numeric":        "DECIMAL",
    "float":          "DOUBLE",
    "real":           "FLOAT",
    "money":          "DECIMAL(19,4)",
    "smallmoney":     "DECIMAL(10,4)",
    "datetime":       "DATETIME",
    "datetime2":      "DATETIME(6)",
    "smalldatetime":  "DATETIME",
    "date":           "DATE",
    "time":           "TIME",
    "datetimeoffset": "DATETIME(6)",
    "timestamp":      "TIMESTAMP",
    "uniqueidentifier":"CHAR(36)",
    "varbinary":      "LONGBLOB",
    "binary":         "BINARY",
    "image":          "LONGBLOB",
    "xml":            "LONGTEXT",
    "geography":      "LONGTEXT",
    "geometry":       "LONGTEXT",
    "hierarchyid":    "VARCHAR(255)",
    "sql_variant":    "LONGTEXT",
}

def convert_type(sql_type: str, max_len, precision, scale) -> str:
    base = sql_type.lower().strip()
    mysql = TYPE_MAP.get(base, sql_type.upper())

    if mysql in ("VARCHAR", "CHAR") and max_len is not None:
        ml = int(max_len)
        if ml == -1:
            # NVARCHAR(MAX) / VARCHAR(MAX) → kein VARCHAR-Limit möglich
            return "LONGTEXT"
        # utf8mb4: max. 4 Bytes/Zeichen → VARCHAR-Limit = 16383 Zeichen
        # Darüber: TEXT (bis ~65K Zeichen), MEDIUMTEXT (bis ~16M), LONGTEXT (bis ~4G)
        if mysql == "CHAR":
            return f"CHAR({min(ml, 255)})"   # CHAR-Limit ist 255
        if ml > 16383:
            return "TEXT" if ml <= 65535 else "MEDIUMTEXT"
        return f"VARCHAR({ml})"

    if mysql == "DECIMAL" and precision:
        sc = scale or 0
        return f"DECIMAL({precision},{sc})"
    return mysql

def mssql_name(name: str) -> str:
    """SQL-Server-Bezeichner (eckige Klammern) → MySQL-Backtick."""
    return f"`{name.strip('[]')}`"

def convert_default(default_val: Optional[str]) -> Optional[str]:
    if default_val is None:
        return None
    d = default_val.strip().strip("()")
    lower = d.lower()
    if lower in ("getdate()", "getutcdate()"):
        return "CURRENT_TIMESTAMP"
    if lower in ("newid()",):
        return None          # UUID() als DEFAULT wird in MySQL nur ab 8.x unterstützt
    if lower == "1":
        return "'1'"
    if lower == "0":
        return "'0'"
    # Einfache Literale
    d = d.strip("'\"")
    return f"'{d}'" if d else None


# ════════════════════════════════════════════════════════════════════════════
#  SQL Server Zugriff
# ════════════════════════════════════════════════════════════════════════════
def get_mssql_drivers() -> List[str]:
    """Gibt verfügbare SQL-Server-ODBC-Treiber zurück.
    Moderne Treiber (ODBC Driver 18/17) werden bevorzugt – der alte
    'SQL Server'-Treiber unterstützt keine LocalDB-Verbindungen."""
    if not PYODBC_OK:
        return []
    all_drivers = pyodbc.drivers()
    # Moderne Treiber zuerst (höchste Versionsnummer vorne)
    modern = sorted(
        [d for d in all_drivers if d.startswith("ODBC Driver") and "SQL Server" in d],
        key=lambda d: [int(x) for x in re.findall(r'\d+', d)],
        reverse=True,
    )
    # Legacy-Treiber ans Ende (nur als Fallback)
    legacy = [d for d in all_drivers if d == "SQL Server"]
    return modern + legacy

def _build_conn_str(driver: str, database: Optional[str] = None) -> str:
    """Erstellt den ODBC-Connection-String passend zum gewählten Treiber."""
    parts = [
        f"DRIVER={{{driver}}}",
        "SERVER=(localdb)\\MSSQLLocalDB",
        "Trusted_Connection=yes",
        "AutoTranslate=no",
    ]
    # ODBC Driver 18 erzwingt verschlüsselte Verbindungen – für LocalDB deaktivieren
    if "18" in driver:
        parts.append("Encrypt=no")
        parts.append("TrustServerCertificate=yes")
    if database:
        parts.append(f"DATABASE={database}")
    return ";".join(parts) + ";"

def _find_ldf(mdf_path: str) -> Optional[str]:
    """Sucht die passende .ldf-Datei zur .mdf-Datei.
    SQL Server benennt Log-Dateien als '<Name>_log.ldf' oder '<Name>.ldf'."""
    base = os.path.splitext(mdf_path)[0]
    for candidate in [base + "_log.ldf", base + ".ldf"]:
        if os.path.isfile(candidate):
            return candidate
    return None

def _win_path(path: str) -> str:
    """Konvertiert Pfad-Separatoren zu Windows-Backslashes (SQL Server-Anforderung)."""
    return os.path.normpath(path).replace("/", "\\")

class MdfSession:
    """Hält pyodbc-Connection + Cleanup-Infos zusammen.
    pyodbc.Connection ist ein C-Extension-Objekt und erlaubt keine
    dynamischen Attribute – daher dieser schlanke Wrapper."""
    def __init__(self, conn: "pyodbc.Connection", db_name: str,
                 driver: str, tmp_dir: str) -> None:
        self.conn    = conn
        self.db_name = db_name
        self.driver  = driver
        self.tmp_dir = tmp_dir

    # Cursor-Delegation damit read_schema() unverändert bleibt
    def cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()


def attach_mdf(mdf_path: str, db_name: str, driver: str, log) -> MdfSession:
    """Hängt eine KOPIE der .mdf-Datei an LocalDB an.
    Die Original-Datei wird nie verändert."""
    import shutil, tempfile

    log(f"Verbinde mit LocalDB via Treiber: {driver}")

    # ── LocalDB starten ───────────────────────────────────────────────────
    try:
        result = _subprocess.run(
            ["SqlLocalDB", "start", "MSSQLLocalDB"],
            capture_output=True, text=True, timeout=15
        )
        log(f"LocalDB: {result.stdout.strip() or result.stderr.strip() or 'gestartet'}")
    except Exception as e:
        log(f"LocalDB-Start übersprungen ({e})")

    # ── Temporäre Kopie erstellen (Original bleibt unberührt) ─────────────
    tmp_dir = tempfile.mkdtemp(prefix="mdf_migration_")
    tmp_mdf = os.path.join(tmp_dir, os.path.basename(mdf_path))
    log(f"Erstelle temporäre Kopie: {tmp_mdf}")
    shutil.copy2(mdf_path, tmp_mdf)

    ldf_path = _find_ldf(mdf_path)
    tmp_ldf  = None
    if ldf_path:
        tmp_ldf = os.path.join(tmp_dir, os.path.basename(ldf_path))
        shutil.copy2(ldf_path, tmp_ldf)
        log(f"LDF-Kopie: {tmp_ldf}")

    # ── Vorherige gleichnamige DB ggf. detachen ───────────────────────────
    master_conn = pyodbc.connect(_build_conn_str(driver), autocommit=True)
    cur = master_conn.cursor()
    cur.execute("SELECT name FROM sys.databases WHERE name = ?", db_name)
    if cur.fetchone():
        log(f"Detache vorhandene DB [{db_name}] …")
        cur.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        cur.execute(f"EXEC sp_detach_db '{db_name}', 'true'")

    # ── Kopie anhängen ────────────────────────────────────────────────────
    mdf_win = _win_path(tmp_mdf).replace("'", "''")
    if tmp_ldf:
        ldf_win = _win_path(tmp_ldf).replace("'", "''")
        log(f"Hänge Kopie an (MDF + LDF) …")
        sql = (
            f"CREATE DATABASE [{db_name}] ON "
            f"(FILENAME='{mdf_win}'), "
            f"(FILENAME='{ldf_win}') "
            f"FOR ATTACH"
        )
    else:
        log(f"Hänge Kopie an (nur MDF, Log wird neu erstellt) …")
        sql = (
            f"CREATE DATABASE [{db_name}] ON "
            f"(FILENAME='{mdf_win}') "
            f"FOR ATTACH_REBUILD_LOG"
        )

    cur.execute(sql)
    log("Temporäre Datenbank angehängt.")
    master_conn.close()

    conn = pyodbc.connect(_build_conn_str(driver, database=db_name))
    return MdfSession(conn, db_name, driver, tmp_dir)


def detach_and_cleanup(session: MdfSession, log) -> None:
    """Detacht die temporäre DB und löscht die Kopien."""
    import shutil
    try:
        session.close()
    except Exception:
        pass
    try:
        mc = pyodbc.connect(_build_conn_str(session.driver), autocommit=True)
        mc.cursor().execute(
            f"ALTER DATABASE [{session.db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
        )
        mc.cursor().execute(f"EXEC sp_detach_db '{session.db_name}', 'true'")
        mc.close()
        log(f"Temporäre DB [{session.db_name}] detacht.")
    except Exception as e:
        log(f"Detach-Warnung: {e}")
    if os.path.isdir(session.tmp_dir):
        try:
            shutil.rmtree(session.tmp_dir)
            log(f"Temp-Kopien gelöscht: {session.tmp_dir}")
        except Exception as e:
            log(f"Temp-Löschen fehlgeschlagen: {e}")


def read_schema(conn: pyodbc.Connection, log) -> dict:
    """Liest Tabellen, Spalten, PKs, FKs und Views aus SQL Server."""
    cur = conn.cursor()
    schema = {"tables": {}, "views": {}}

    # ── Tabellen & Spalten ──
    log("Lese Tabellendefinitionen …")
    cur.execute("""
        SELECT
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.ORDINAL_POSITION,
            c.IS_NULLABLE,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA+'.'+t.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
    """)
    for row in cur.fetchall():
        tschema, tname, col, pos, nullable, dtype, maxlen, prec, scale, default, is_id = row
        key = f"{tschema}.{tname}"
        if key not in schema["tables"]:
            schema["tables"][key] = {"schema": tschema, "name": tname, "columns": [], "pk": [], "fk": []}
        schema["tables"][key]["columns"].append({
            "name": col, "pos": pos, "nullable": nullable == "YES",
            "type": dtype, "max_len": maxlen, "precision": prec, "scale": scale,
            "default": default, "identity": bool(is_id),
        })

    # ── Primary Keys ──
    log("Lese Primary Keys …")
    cur.execute("""
        SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
           AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.ORDINAL_POSITION
    """)
    for tschema, tname, col in cur.fetchall():
        key = f"{tschema}.{tname}"
        if key in schema["tables"]:
            schema["tables"][key]["pk"].append(col)

    # ── Foreign Keys ──
    log("Lese Foreign Keys …")
    cur.execute("""
        SELECT
            fk.name AS fk_name,
            OBJECT_SCHEMA_NAME(fkc.parent_object_id)      AS from_schema,
            OBJECT_NAME(fkc.parent_object_id)             AS from_table,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id)      AS from_col,
            OBJECT_SCHEMA_NAME(fkc.referenced_object_id)  AS to_schema,
            OBJECT_NAME(fkc.referenced_object_id)         AS to_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS to_col
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        ORDER BY from_schema, from_table
    """)
    for row in cur.fetchall():
        fk_name, fs, ft, fc, ts, tt, tc = row
        key = f"{fs}.{ft}"
        if key in schema["tables"]:
            schema["tables"][key]["fk"].append({
                "name": fk_name, "from_col": fc,
                "to_schema": ts, "to_table": tt, "to_col": tc,
            })

    # ── Views ──
    # INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION ist auf 4000 Zeichen begrenzt →
    # sys.sql_modules.definition ist nvarchar(MAX) und liefert den vollständigen Text.
    log("Lese View-Definitionen …")
    cur.execute("""
        SELECT
            OBJECT_SCHEMA_NAME(v.object_id) AS vschema,
            v.name                           AS vname,
            m.definition                     AS vdef
        FROM sys.views        v
        JOIN sys.sql_modules  m ON m.object_id = v.object_id
        ORDER BY vschema, vname
    """)
    for vschema, vname, vdef in cur.fetchall():
        schema["views"][f"{vschema}.{vname}"] = {
            "schema": vschema, "name": vname, "definition": vdef or ""
        }

    log(f"Schema gelesen: {len(schema['tables'])} Tabellen, {len(schema['views'])} Views.")
    return schema


# ════════════════════════════════════════════════════════════════════════════
#  Schema → MySQL DDL
# ════════════════════════════════════════════════════════════════════════════
def generate_mysql_ddl(schema: dict, target_db: str) -> str:
    lines = [
        f"-- Generiert von MDF-to-MySQL Migration Tool",
        f"-- Quelldatenbank aus .mdf → Ziel: {target_db}",
        "",
        f"CREATE DATABASE IF NOT EXISTS `{target_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        f"USE `{target_db}`;",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]

    # Tabellen
    for tinfo in schema["tables"].values():
        tname = tinfo["name"]
        col_defs = []
        for c in tinfo["columns"]:
            mysql_type = convert_type(c["type"], c["max_len"], c["precision"], c["scale"])
            null_str   = "" if c["nullable"] else " NOT NULL"
            auto_str   = " AUTO_INCREMENT" if c["identity"] else ""
            default    = convert_default(c["default"]) if not c["identity"] else None
            def_str    = f" DEFAULT {default}" if default else ""
            col_defs.append(f"  {mssql_name(c['name'])} {mysql_type}{null_str}{auto_str}{def_str}")

        if tinfo["pk"]:
            pk_cols = ", ".join(mssql_name(p) for p in tinfo["pk"])
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        lines.append(f"DROP TABLE IF EXISTS {mssql_name(tname)};")
        lines.append(f"CREATE TABLE {mssql_name(tname)} (")
        lines.append(",\n".join(col_defs))
        lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        lines.append("")

    # Foreign Keys (separat, nach allen Tabellen)
    for tinfo in schema["tables"].values():
        for fk in tinfo["fk"]:
            safe_fk = re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"])
            lines.append(
                f"ALTER TABLE {mssql_name(tinfo['name'])} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({mssql_name(fk['from_col'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({mssql_name(fk['to_col'])});"
            )
    if any(tinfo["fk"] for tinfo in schema["tables"].values()):
        lines.append("")

    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    lines.append("")

    # Views — topologisch sortiert, damit Abhängigkeiten zuerst erstellt werden
    sorted_views = _topo_sort_views(schema["views"])
    for vinfo in sorted_views:
        vname = vinfo["name"]
        vdef, warnings = convert_view_sql(vinfo["definition"])
        lines.append(f"DROP VIEW IF EXISTS {mssql_name(vname)};")
        if warnings:
            for w in warnings:
                lines.append(f"-- ⚠ {w}")
        lines.append(f"CREATE VIEW {mssql_name(vname)} AS")
        lines.append(vdef + ";")
        lines.append("")

    return "\n".join(lines)


def _topo_sort_views(views: dict) -> list:
    """Sortiert Views topologisch: zuerst Views ohne Abhängigkeiten,
    dann Views die andere Views referenzieren."""
    view_names = {v["name"].lower() for v in views.values()}

    # Abhängigkeiten jeder View ermitteln: welche anderen Views werden referenziert?
    deps: Dict[str, set] = {}
    for key, vinfo in views.items():
        body = vinfo["definition"].lower()
        found = set()
        for other_name in view_names:
            if other_name == vinfo["name"].lower():
                continue
            # Suche nach dem View-Namen als eigenständiges Wort im SQL-Body
            if re.search(r'\b' + re.escape(other_name) + r'\b', body):
                found.add(other_name)
        deps[vinfo["name"].lower()] = found

    # Kahn's Algorithmus für topologischen Sort
    sorted_list: list = []
    remaining = {v["name"].lower(): v for v in views.values()}
    iterations = 0
    while remaining and iterations < len(views) + 1:
        iterations += 1
        # Views ohne ausstehende Abhängigkeiten zuerst
        ready = [
            name for name, vinfo in remaining.items()
            if not (deps[name] & set(remaining.keys()) - {name})
        ]
        if not ready:
            # Zyklus erkannt — restliche Views einfach anhängen
            ready = list(remaining.keys())
        for name in sorted(ready):  # deterministisch sortieren
            sorted_list.append(remaining.pop(name))

    return sorted_list


def convert_view_sql(tsql: str) -> tuple:
    """T-SQL → MySQL Konvertierung für View-Definitionen.
    Gibt (sql, warnings) zurück – warnings ist eine Liste von Strings
    für Konstrukte die manuell nachbearbeitet werden müssen."""
    sql = tsql.strip()
    warnings: List[str] = []

    # ── CREATE VIEW Header entfernen ──────────────────────────────────────
    # sys.sql_modules liefert den vollständigen T-SQL-Text inkl.
    # "CREATE VIEW [dbo].[Name] AS" – das wird von uns neu generiert.
    # KEIN DOTALL: Verhindert, dass der Regex über Zeilenenden hinaus greift
    # und Teile des SELECT-Bodys mitfrisst.
    # View-Namen dürfen Leerzeichen nur innerhalb von [] enthalten.
    sql = re.sub(
        r'^\s*CREATE\s+VIEW\s+'              # CREATE VIEW
        r'(?:\[[\w\s]+\]|\w+)'              # Schema: [dbo] oder dbo
        r'(?:\.(?:\[[\w\s]+\]|\w+))?'       # optionaler .ViewName-Teil
        r'\s*(?:\([^)]*\))?\s*\bAS\b\s*',   # optionale Spaltenliste + AS
        '',
        sql,
        count=1,
        flags=re.IGNORECASE,                 # kein DOTALL – schützt SELECT-Body
    ).strip()

    # ── Schema-Präfixe entfernen: [dbo]. und dbo. → nichts ───────────────
    sql = re.sub(r'\[dbo\]\.', '', sql)
    sql = re.sub(r'\bdbo\.', '', sql)

    # Bezeichner: [Name] → `Name`
    sql = re.sub(r'\[([^\]]+)\]', r'`\1`', sql)

    # ── SQL-Server-Datentypen in View-Körpern ersetzen ────────────────────
    # Tritt auf in CAST(x AS money), CONVERT(money, x) usw.
    _VIEW_TYPE_MAP = [
        (r'\bmoney\b',            'DECIMAL(19,4)'),
        (r'\bsmallmoney\b',       'DECIMAL(10,4)'),
        (r'\bnvarchar\b',         'CHAR'),
        (r'\bnchar\b',            'CHAR'),
        (r'\bntext\b',            'TEXT'),
        (r'\bdatetime2\b',        'DATETIME'),
        (r'\bsmalldatetime\b',    'DATETIME'),
        (r'\bdatetimeoffset\b',   'DATETIME'),
        (r'\buniqueidentifier\b', 'CHAR(36)'),
        (r'\bbit\b',              'TINYINT(1)'),
        (r'\bimage\b',            'LONGBLOB'),
        (r'\bsql_variant\b',      'TEXT'),
    ]
    for pattern, replacement in _VIEW_TYPE_MAP:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    # ── T-SQL Funktionen → MySQL ──────────────────────────────────────────
    sql = re.sub(r'\bGETDATE\s*\(\s*\)',    'NOW()',           sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGETUTCDATE\s*\(\s*\)','UTC_TIMESTAMP()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bISNULL\s*\(',          'IFNULL(',         sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bIIF\s*\(',             'IF(',             sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLEN\s*\(',             'LENGTH(',         sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCHARINDEX\s*\(([^,]+),([^)]+)\)',
                 r'LOCATE(\1,\2)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSUBSTRING\s*\(',      'SUBSTRING(',      sql, flags=re.IGNORECASE)
    # CONVERT: MySQL hat dieselbe Syntax CONVERT(expr, type) wie T-SQL –
    # Typnamen wurden bereits oben ersetzt, kein weiterer Umbau nötig.

    # WITH (NOLOCK) entfernen
    sql = re.sub(r'\bWITH\s*\(\s*NOLOCK\s*\)', '', sql, flags=re.IGNORECASE)

    # TOP n entfernen (kein direktes Äquivalent ohne LIMIT-Position)
    sql = re.sub(r'\bTOP\s+\d+\b', '', sql, flags=re.IGNORECASE)

    # ── STRING_AGG → GROUP_CONCAT ─────────────────────────────────────────
    # T-SQL:  STRING_AGG(x, ', ') WITHIN GROUP (ORDER BY x)
    # MySQL:  GROUP_CONCAT(x ORDER BY x SEPARATOR ', ')
    def _string_agg_repl(m: re.Match) -> str:
        expr      = m.group(1).strip()
        separator = m.group(2).strip().strip("'\"")
        order_col = m.group(3).strip() if m.group(3) else None
        if order_col:
            return f"GROUP_CONCAT({expr} ORDER BY {order_col} SEPARATOR '{separator}')"
        return f"GROUP_CONCAT({expr} SEPARATOR '{separator}')"

    # Mit WITHIN GROUP (häufig)
    sql = re.sub(
        r'\bSTRING_AGG\s*\(\s*(.+?)\s*,\s*([\'"][^\'"]*[\'"])\s*\)'
        r'\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\s*\)',
        _string_agg_repl,
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Ohne WITHIN GROUP (seltener)
    sql = re.sub(
        r'\bSTRING_AGG\s*\(\s*(.+?)\s*,\s*([\'"][^\'"]*[\'"])\s*\)',
        lambda m: f"GROUP_CONCAT({m.group(1).strip()} SEPARATOR "
                  f"'{m.group(2).strip().strip(chr(39)+chr(34))}')",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # ── OUTER APPLY → LEFT JOIN LATERAL (...) ON TRUE ────────────────────
    # T-SQL:  OUTER APPLY (subquery) alias
    # MySQL:  LEFT JOIN LATERAL (subquery) alias ON TRUE
    sql = re.sub(
        r'\bOUTER\s+APPLY\s*\(',
        'LEFT JOIN LATERAL (',
        sql,
        flags=re.IGNORECASE,
    )
    # Alias nach der schließenden Klammer braucht kein ON TRUE – das fügen wir
    # hinter dem Alias ein. Da wir den Alias nicht kennen, ergänzen wir ON TRUE
    # nach dem Alias-Token mit einem zweiten Pass.
    sql = re.sub(
        r'(LEFT\s+JOIN\s+LATERAL\s*\(.*?\)\s*)(\w+)(?!\s+ON\b)',
        r'\1\2 ON TRUE',
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # ── CROSS APPLY → JOIN LATERAL (...) ON TRUE ─────────────────────────
    # T-SQL:  CROSS APPLY (subquery) alias
    # MySQL:  JOIN LATERAL (subquery) alias ON TRUE
    sql = re.sub(
        r'\bCROSS\s+APPLY\s*\(',
        'JOIN LATERAL (',
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r'((?<!LEFT\s)JOIN\s+LATERAL\s*\(.*?\)\s*)(\w+)(?!\s+ON\b)',
        r'\1\2 ON TRUE',
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # Doppelte Leerzeilen bereinigen
    sql = re.sub(r'\n{3,}', '\n\n', sql)

    return sql.strip(), warnings


# ════════════════════════════════════════════════════════════════════════════
#  MySQL Deploy
# ════════════════════════════════════════════════════════════════════════════
def deploy_to_mysql(ddl: str, host: str, port: int, user: str, password: str,
                    target_db: str, log) -> None:
    log(f"Verbinde mit MySQL {host}:{port} …")
    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        allow_local_infile=True, charset="utf8mb4",
        connection_timeout=10,
    )
    cur = conn.cursor()

    statements = [s.strip() for s in ddl.split(";") if s.strip()]
    total = len(statements)
    log(f"{total} SQL-Anweisungen werden ausgeführt …")

    errors = []
    for i, stmt in enumerate(statements, 1):
        try:
            cur.execute(stmt)
            conn.commit()
        except mysql.connector.Error as e:
            errors.append(f"[{i}/{total}] {e}\n  SQL: {stmt[:120]}")

    cur.close()
    conn.close()

    if errors:
        log(f"\n⚠ {len(errors)} Fehler aufgetreten:")
        for err in errors:
            log("  " + err)
    else:
        log(f"\n✓ Alle {total} Anweisungen erfolgreich ausgeführt.")


# ════════════════════════════════════════════════════════════════════════════
#  GUI
# ════════════════════════════════════════════════════════════════════════════
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MDF → MySQL Migration Tool")
        self.geometry("820x700")
        self.resizable(True, True)
        self._build_ui()
        self._load_config()   # gespeicherte Eingaben wiederherstellen
        self._check_deps()

    # ── UI-Aufbau ────────────────────────────────────────────────────────
    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        self.tab_src   = ttk.Frame(nb)
        self.tab_dst   = ttk.Frame(nb)
        self.tab_ddl   = ttk.Frame(nb)
        self.tab_log   = ttk.Frame(nb)

        nb.add(self.tab_src, text=" 1 · Quelle (.mdf) ")
        nb.add(self.tab_dst, text=" 2 · Ziel (MySQL)  ")
        nb.add(self.tab_ddl, text=" 3 · DDL-Vorschau  ")
        nb.add(self.tab_log, text=" 4 · Log           ")

        self._build_source_tab()
        self._build_dest_tab()
        self._build_ddl_tab()
        self._build_log_tab()

        # Aktions-Buttons unten
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=8, pady=(0, 4))

        ttk.Button(btn_frame, text="Schema lesen",         command=self._read_schema).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="DDL generieren",       command=self._generate_ddl).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="DDL speichern …",      command=self._save_ddl).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="▶ Auf MySQL deployen", command=self._deploy).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Abhängigkeiten prüfen",command=self._check_deps).pack(side="right", padx=4)

        # Konfig-Leiste
        cfg_frame = ttk.Frame(self)
        cfg_frame.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Label(cfg_frame, text="Konfiguration:").pack(side="left", padx=(4, 8))

        # Profil-Name
        ttk.Label(cfg_frame, text="Profil:").pack(side="left")
        self._profile_var = tk.StringVar(value="Standard")
        self._profile_combo = ttk.Combobox(cfg_frame, textvariable=self._profile_var, width=18)
        self._profile_combo.pack(side="left", padx=4)

        ttk.Button(cfg_frame, text="💾  Speichern", command=self._save_config).pack(side="left", padx=4)
        ttk.Button(cfg_frame, text="📂  Laden",     command=self._load_config).pack(side="left", padx=4)
        ttk.Button(cfg_frame, text="🗑  Löschen",   command=self._delete_profile).pack(side="left", padx=4)

        self._cfg_status = ttk.Label(cfg_frame, text="", foreground="#555")
        self._cfg_status.pack(side="left", padx=8)

        self._refresh_profiles()

    def _build_source_tab(self):
        f = self.tab_src
        ttk.Label(f, text=".mdf Datei:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.mdf_path = tk.StringVar()
        ttk.Entry(f, textvariable=self.mdf_path, width=55).grid(row=0, column=1, padx=4, pady=6)
        ttk.Button(f, text="Durchsuchen …", command=self._browse_mdf).grid(row=0, column=2, padx=4)

        ttk.Label(f, text="Datenbank-Name (intern):").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.db_attach_name = tk.StringVar(value="MigratedDB")
        ttk.Entry(f, textvariable=self.db_attach_name, width=30).grid(row=1, column=1, sticky="w", padx=4)

        ttk.Label(f, text="ODBC-Treiber:").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        self.driver_var = tk.StringVar()
        self.driver_combo = ttk.Combobox(f, textvariable=self.driver_var, width=52)
        self.driver_combo.grid(row=2, column=1, padx=4, pady=6)
        ttk.Button(f, text="Treiber aktualisieren", command=self._refresh_drivers).grid(row=2, column=2, padx=4)

        info = (
            "Hinweis: Zum Lesen der .mdf-Datei wird Microsoft SQL Server LocalDB\n"
            "oder SQL Server Express benötigt (kostenlos bei Microsoft erhältlich).\n"
            "Installer: https://aka.ms/sqllocaldb\n\n"
            "Alternativ: DDL-Datei manuell aus SQL Server Management Studio exportieren\n"
            "und im Tab '3 · DDL-Vorschau' einfügen."
        )
        ttk.Label(f, text=info, foreground="#555", justify="left").grid(
            row=3, column=0, columnspan=3, padx=8, pady=12, sticky="w")

        self._refresh_drivers()

    def _build_dest_tab(self):
        f = self.tab_dst
        fields = [
            ("MySQL Host (Synology IP):", "mysql_host", "192.168.1.x"),
            ("Port:",                     "mysql_port", "3306"),
            ("Benutzer:",                 "mysql_user", "root"),
            ("Passwort:",                 "mysql_pass", ""),
            ("Ziel-Datenbankname:",       "mysql_db",   "migrated_db"),
        ]
        for i, (label, attr, placeholder) in enumerate(fields):
            ttk.Label(f, text=label).grid(row=i, column=0, sticky="w", padx=8, pady=6)
            var = tk.StringVar(value=placeholder if attr != "mysql_pass" else "")
            setattr(self, attr, var)
            show = "*" if attr == "mysql_pass" else ""
            ttk.Entry(f, textvariable=var, width=40, show=show).grid(row=i, column=1, padx=4, pady=6, sticky="w")

        ttk.Button(f, text="Verbindung testen", command=self._test_mysql).grid(
            row=len(fields), column=1, sticky="w", padx=4, pady=10)

        ttk.Label(f,
            text="Synology: MariaDB/MySQL-Paket im Paket-Zentrum aktivieren,\n"
                 "Remote-Zugriff in phpMyAdmin oder SSH erlauben.",
            foreground="#555", justify="left"
        ).grid(row=len(fields)+1, column=0, columnspan=2, padx=8, pady=8, sticky="w")

    def _build_ddl_tab(self):
        f = self.tab_ddl
        self.ddl_text = scrolledtext.ScrolledText(f, font=("Consolas", 9), wrap="none")
        self.ddl_text.pack(fill="both", expand=True, padx=4, pady=4)
        ttk.Label(f,
            text="DDL hier direkt bearbeiten oder manuell einfügen.",
            foreground="#555"
        ).pack(anchor="w", padx=4)

    def _build_log_tab(self):
        f = self.tab_log

        # Log-Datei Pfad-Anzeige
        path_frame = ttk.Frame(f)
        path_frame.pack(fill="x", padx=4, pady=(4, 0))
        ttk.Label(path_frame, text="Log-Datei:").pack(side="left")
        self._log_path_var = tk.StringVar(value=_LOG_FILE)
        ttk.Entry(path_frame, textvariable=self._log_path_var,
                  state="readonly", width=70).pack(side="left", padx=4)
        ttk.Button(path_frame, text="Im Explorer öffnen",
                   command=self._open_log_folder).pack(side="left", padx=2)

        # Textbereich
        self.log_text = scrolledtext.ScrolledText(
            f, font=("Consolas", 9), state="disabled", wrap="none"
        )
        self.log_text.pack(fill="both", expand=True, padx=4, pady=4)

        # Farbliche Markierung für Fehler/Warnungen/Erfolg
        self.log_text.tag_config("error",   foreground="#cc0000", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("warning", foreground="#b36200")
        self.log_text.tag_config("success", foreground="#006600", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("section", foreground="#00008b", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("ts",      foreground="#888888")

        # Button-Leiste
        btn_frame = ttk.Frame(f)
        btn_frame.pack(fill="x", padx=4, pady=2)
        ttk.Button(btn_frame, text="Log leeren",     command=self._clear_log).pack(side="right", padx=2)
        ttk.Button(btn_frame, text="Log kopieren",   command=self._copy_log).pack(side="right", padx=2)

        # Erste Zeile in Log-Datei schreiben
        with open(_LOG_FILE, "w", encoding="utf-8") as fh:
            fh.write(f"=== MDF-to-MySQL Migration Log  {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===\n")

    # ── Hilfsmethoden ────────────────────────────────────────────────────
    def log(self, msg: str):
        ts    = datetime.datetime.now().strftime("%H:%M:%S")
        lower = msg.lower().strip()

        # Tag für Farbmarkierung ermitteln
        if lower.startswith("fehler") or lower.startswith("error") or "fehler:" in lower:
            tag = "error"
        elif lower.startswith("⚠") or "warnung" in lower or lower.startswith("warning"):
            tag = "warning"
        elif lower.startswith("✓") or "erfolgreich" in lower or lower.startswith("fertig"):
            tag = "success"
        elif lower.startswith("──") or lower.startswith("=="):
            tag = "section"
        else:
            tag = None

        self.log_text.config(state="normal")
        # Zeitstempel (grau)
        self.log_text.insert("end", f"[{ts}] ", "ts")
        # Nachricht (ggf. farbig)
        if tag:
            self.log_text.insert("end", msg + "\n", tag)
        else:
            self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.update_idletasks()

        # Gleichzeitig in Datei schreiben
        try:
            with open(_LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(f"[{ts}] {msg}\n")
        except OSError:
            pass

    def _clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")
        # Log-Datei ebenfalls leeren
        try:
            with open(_LOG_FILE, "w", encoding="utf-8") as fh:
                fh.write(f"=== Log geleert  {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===\n")
        except OSError:
            pass

    def _copy_log(self):
        content = self.log_text.get("1.0", "end").strip()
        self.clipboard_clear()
        self.clipboard_append(content)
        self.log("✓ Log in Zwischenablage kopiert.")

    def _open_log_folder(self):
        _subprocess.Popen(["explorer", "/select,", os.path.normpath(_LOG_FILE)])

    def _browse_mdf(self):
        path = filedialog.askopenfilename(
            title="MDF-Datei auswählen",
            filetypes=[("SQL Server Database", "*.mdf"), ("Alle Dateien", "*.*")]
        )
        if path:
            self.mdf_path.set(path)
            # DB-Name aus Dateiname ableiten
            basename = os.path.splitext(os.path.basename(path))[0]
            self.db_attach_name.set(re.sub(r'[^a-zA-Z0-9_]', '_', basename))

    def _refresh_drivers(self):
        drivers = get_mssql_drivers()
        self.driver_combo["values"] = drivers
        if drivers:
            self.driver_var.set(drivers[0])

    def _check_deps(self):
        msgs = []
        if PYODBC_OK:
            msgs.append("✓ pyodbc installiert")
            drivers = get_mssql_drivers()
            if drivers:
                msgs.append(f"✓ ODBC-Treiber gefunden: {drivers[0]}")
            else:
                msgs.append("⚠ Kein SQL-Server-ODBC-Treiber gefunden")
                msgs.append("  → SQL Server LocalDB installieren: https://aka.ms/sqllocaldb")
        else:
            msgs.append("✗ pyodbc fehlt  → pip install pyodbc")

        if MYSQL_OK:
            msgs.append("✓ mysql-connector-python installiert")
        else:
            msgs.append("✗ mysql-connector-python fehlt  → pip install mysql-connector-python")

        self.log("── Abhängigkeiten ──")
        for m in msgs:
            self.log("  " + m)
        self.log("")

    # ── Aktionen ────────────────────────────────────────────────────────
    def _read_schema(self):
        if not PYODBC_OK:
            messagebox.showerror("Fehler", "pyodbc nicht installiert.\npip install pyodbc")
            return
        mdf = self.mdf_path.get().strip()
        if not mdf or not os.path.isfile(mdf):
            messagebox.showerror("Fehler", "Bitte eine gültige .mdf-Datei auswählen.")
            return
        driver = self.driver_var.get()
        if not driver:
            messagebox.showerror("Fehler", "Kein ODBC-Treiber ausgewählt.")
            return

        def task():
            session = None
            try:
                self.log(f"── Schema lesen: {mdf}")
                self.log("Original-Datei wird nicht verändert – Tool arbeitet auf Kopie.")
                session = attach_mdf(mdf, self.db_attach_name.get(), driver, self.log)
                self._schema = read_schema(session, self.log)
                self.log("Schema erfolgreich gelesen. → DDL generieren klicken.")
            except Exception as e:
                self.log(f"FEHLER: {e}")
                messagebox.showerror("Fehler", str(e))
            finally:
                if session is not None:
                    detach_and_cleanup(session, self.log)

        threading.Thread(target=task, daemon=True).start()

    def _generate_ddl(self):
        if not hasattr(self, "_schema"):
            messagebox.showinfo("Hinweis", "Bitte zuerst 'Schema lesen' ausführen.")
            return
        target_db = self.mysql_db.get().strip() or "migrated_db"
        self.log(f"Generiere DDL für Zieldatenbank '{target_db}' …")
        ddl = generate_mysql_ddl(self._schema, target_db)
        self.ddl_text.delete("1.0", "end")
        self.ddl_text.insert("1.0", ddl)
        tcount = len(self._schema["tables"])
        vcount = len(self._schema["views"])
        self.log(f"DDL generiert: {tcount} Tabellen, {vcount} Views. Prüfe Tab '3 · DDL-Vorschau'.")

    def _save_ddl(self):
        ddl = self.ddl_text.get("1.0", "end").strip()
        if not ddl:
            messagebox.showinfo("Hinweis", "DDL-Vorschau ist leer.")
            return
        path = filedialog.asksaveasfilename(
            title="DDL speichern",
            defaultextension=".sql",
            filetypes=[("SQL-Datei", "*.sql"), ("Alle Dateien", "*.*")]
        )
        if path:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(ddl)
            self.log(f"DDL gespeichert: {path}")

    def _test_mysql(self):
        if not MYSQL_OK:
            messagebox.showerror("Fehler", "mysql-connector-python nicht installiert.\npip install mysql-connector-python")
            return
        try:
            conn = mysql.connector.connect(
                host=self.mysql_host.get().strip(),
                port=int(self.mysql_port.get().strip()),
                user=self.mysql_user.get().strip(),
                password=self.mysql_pass.get(),
                connection_timeout=5,
            )
            conn.close()
            self.log("✓ MySQL-Verbindung erfolgreich.")
            messagebox.showinfo("Verbindung OK", "MySQL-Verbindung erfolgreich!")
        except Exception as e:
            self.log(f"Verbindungsfehler: {e}")
            messagebox.showerror("Verbindungsfehler", str(e))

    def _deploy(self):
        if not MYSQL_OK:
            messagebox.showerror("Fehler", "mysql-connector-python nicht installiert.\npip install mysql-connector-python")
            return
        ddl = self.ddl_text.get("1.0", "end").strip()
        if not ddl:
            messagebox.showinfo("Hinweis", "DDL-Vorschau ist leer. Bitte zuerst DDL generieren.")
            return
        if not messagebox.askyesno(
            "Deployment bestätigen",
            f"DDL auf {self.mysql_host.get()}:{self.mysql_port.get()}\n"
            f"Datenbank: {self.mysql_db.get()}\n\nJetzt ausführen?"
        ):
            return

        def task():
            try:
                deploy_to_mysql(
                    ddl,
                    host=self.mysql_host.get().strip(),
                    port=int(self.mysql_port.get().strip()),
                    user=self.mysql_user.get().strip(),
                    password=self.mysql_pass.get(),
                    target_db=self.mysql_db.get().strip(),
                    log=self.log,
                )
            except Exception as e:
                self.log(f"FEHLER beim Deployment: {e}")
                messagebox.showerror("Fehler", str(e))

        threading.Thread(target=task, daemon=True).start()

    # ── Konfiguration ────────────────────────────────────────────────────
    def _all_profiles(self) -> dict:
        """Liest alle Profile aus config.json. Gibt leeres Dict zurück falls nicht vorhanden."""
        if os.path.isfile(_CFG_FILE):
            try:
                with open(_CFG_FILE, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _refresh_profiles(self):
        """Aktualisiert die Profil-Auswahlliste."""
        profiles = list(self._all_profiles().keys())
        self._profile_combo["values"] = profiles or ["Standard"]
        if not self._profile_var.get() and profiles:
            self._profile_var.set(profiles[0])

    def _save_config(self):
        """Speichert alle Eingabefelder unter dem gewählten Profilnamen."""
        profile = self._profile_var.get().strip() or "Standard"

        # Passwort minimal verschleiern (kein Klartext in JSON)
        pw_obf = base64.b64encode(self.mysql_pass.get().encode()).decode()

        data = {
            "mdf_path":       self.mdf_path.get(),
            "db_attach_name": self.db_attach_name.get(),
            "driver":         self.driver_var.get(),
            "mysql_host":     self.mysql_host.get(),
            "mysql_port":     self.mysql_port.get(),
            "mysql_user":     self.mysql_user.get(),
            "mysql_pass_b64": pw_obf,
            "mysql_db":       self.mysql_db.get(),
            "saved_at":       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        all_cfg = self._all_profiles()
        all_cfg[profile] = data

        with open(_CFG_FILE, "w", encoding="utf-8") as fh:
            json.dump(all_cfg, fh, ensure_ascii=False, indent=2)

        self._refresh_profiles()
        self._profile_var.set(profile)
        msg = f"✓ Profil '{profile}' gespeichert."
        self._cfg_status.config(text=msg, foreground="#006600")
        self.log(msg)
        self.after(3000, lambda: self._cfg_status.config(text=""))

    def _load_config(self, profile: Optional[str] = None):
        """Lädt ein Profil und füllt alle Felder. Ohne Argument: zuletzt genutztes Profil."""
        all_cfg = self._all_profiles()
        if not all_cfg:
            return

        if profile is None:
            # Beim Start: Profil aus config.json laden das zuletzt gespeichert wurde
            profile = self._profile_var.get().strip()
            if profile not in all_cfg:
                profile = next(iter(all_cfg))   # erstes verfügbares

        if profile not in all_cfg:
            self.log(f"⚠ Profil '{profile}' nicht gefunden.")
            return

        d = all_cfg[profile]

        self.mdf_path.set(       d.get("mdf_path",       ""))
        self.db_attach_name.set( d.get("db_attach_name", "MigratedDB"))
        self.mysql_host.set(     d.get("mysql_host",     ""))
        self.mysql_port.set(     d.get("mysql_port",     "3306"))
        self.mysql_user.set(     d.get("mysql_user",     ""))
        self.mysql_db.set(       d.get("mysql_db",       ""))

        # Passwort entschlüsseln
        try:
            pw = base64.b64decode(d.get("mysql_pass_b64", "")).decode()
        except Exception:
            pw = ""
        self.mysql_pass.set(pw)

        # Treiber setzen (nach refresh, damit Combobox-Werte bekannt sind)
        saved_driver = d.get("driver", "")
        if saved_driver:
            self.driver_var.set(saved_driver)

        self._profile_var.set(profile)
        self._refresh_profiles()

        ts = d.get("saved_at", "")
        msg = f"✓ Profil '{profile}' geladen  (gespeichert: {ts})"
        self._cfg_status.config(text=f"Profil '{profile}' geladen", foreground="#006600")
        self.log(msg)
        self.after(4000, lambda: self._cfg_status.config(text=""))

    def _delete_profile(self):
        profile = self._profile_var.get().strip()
        if not profile:
            return
        if not messagebox.askyesno("Profil löschen", f"Profil '{profile}' wirklich löschen?"):
            return

        all_cfg = self._all_profiles()
        if profile in all_cfg:
            del all_cfg[profile]
            with open(_CFG_FILE, "w", encoding="utf-8") as fh:
                json.dump(all_cfg, fh, ensure_ascii=False, indent=2)
            self.log(f"Profil '{profile}' gelöscht.")

        self._refresh_profiles()
        if all_cfg:
            self._profile_var.set(next(iter(all_cfg)))
        else:
            self._profile_var.set("Standard")


# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = App()
    app.mainloop()

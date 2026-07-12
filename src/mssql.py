"""
SQL Server Zugriff.
Enthält:
  - Treiber-Erkennung  (get_mssql_drivers)
  - Verbindungsaufbau  (attach_mdf, MdfSession)
  - Schema-Lesen       (read_schema)
  - Cleanup            (detach_and_cleanup)
"""
import os
import re
import subprocess as _subprocess
from typing import Dict, List, Optional

try:
    import pyodbc
    PYODBC_OK = True
except ImportError:
    pyodbc    = None   # type: ignore
    PYODBC_OK = False


# ════════════════════════════════════════════════════════════════════════════
#  Treiber-Hilfen
# ════════════════════════════════════════════════════════════════════════════
def get_mssql_drivers() -> List[str]:
    """Gibt verfügbare SQL-Server-ODBC-Treiber zurück.
    Moderne Treiber (ODBC Driver 18/17) werden bevorzugt – der alte
    'SQL Server'-Treiber unterstützt keine LocalDB-Verbindungen."""
    if not PYODBC_OK:
        return []
    all_drivers = pyodbc.drivers()
    modern = sorted(
        [d for d in all_drivers if d.startswith("ODBC Driver") and "SQL Server" in d],
        key=lambda d: [int(x) for x in re.findall(r'\d+', d)],
        reverse=True,
    )
    legacy = [d for d in all_drivers if d == "SQL Server"]
    return modern + legacy


def _build_conn_str(driver: str, database: Optional[str] = None) -> str:
    """Erstellt den ODBC-Connection-String passend zum gewählten Treiber.

    Umgebungsvariablen (für Docker / Linux):
      MSSQL_SERVER   – Serveradresse (Standard: (localdb)\\MSSQLLocalDB)
      MSSQL_USER     – SQL-Auth Benutzername (optional; leer → Windows-Auth)
      MSSQL_PASS     – SQL-Auth Passwort     (optional; leer → Windows-Auth)
    """
    server    = os.environ.get("MSSQL_SERVER", r"(localdb)\MSSQLLocalDB")
    sql_user  = os.environ.get("MSSQL_USER", "")
    sql_pass  = os.environ.get("MSSQL_PASS", "")

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        "AutoTranslate=no",
    ]

    if sql_user and sql_pass:
        # SQL Server-Authentifizierung (benötigt für Docker / Remote-Server)
        parts += [
            f"UID={sql_user}",
            f"PWD={sql_pass}",
            "Trusted_Connection=no",
        ]
    else:
        # Windows-Authentifizierung (Standard für LocalDB)
        parts.append("Trusted_Connection=yes")

    if "18" in driver:
        parts.append("Encrypt=no")
        parts.append("TrustServerCertificate=yes")
    if database:
        parts.append(f"DATABASE={database}")
    return ";".join(parts) + ";"


def _find_ldf(mdf_path: str) -> Optional[str]:
    """Sucht die passende .ldf-Datei zur .mdf-Datei."""
    base = os.path.splitext(mdf_path)[0]
    for candidate in [base + "_log.ldf", base + ".ldf"]:
        if os.path.isfile(candidate):
            return candidate
    return None


def _win_path(path: str) -> str:
    """Konvertiert Pfad-Separatoren zu Windows-Backslashes (SQL-Server-Anforderung)."""
    return os.path.normpath(path).replace("/", "\\")


# ════════════════════════════════════════════════════════════════════════════
#  MdfSession – Wrapper um pyodbc.Connection
# ════════════════════════════════════════════════════════════════════════════
class MdfSession:
    """Hält pyodbc-Connection + Cleanup-Infos zusammen.
    pyodbc.Connection ist ein C-Extension-Objekt und erlaubt keine
    dynamischen Attribute – daher dieser schlanke Wrapper."""

    def __init__(self, conn, db_name: str, driver: str, tmp_dir: str) -> None:
        self.conn    = conn
        self.db_name = db_name
        self.driver  = driver
        self.tmp_dir = tmp_dir

    def cursor(self):
        return self.conn.cursor()

    def close(self):
        self.conn.close()


# ════════════════════════════════════════════════════════════════════════════
#  Attach / Detach
# ════════════════════════════════════════════════════════════════════════════
def attach_mdf(mdf_path: str, db_name: str, driver: str, log) -> MdfSession:
    """Hängt eine KOPIE der .mdf-Datei an LocalDB an.
    Die Original-Datei wird nie verändert."""
    import shutil
    import tempfile

    log(f"Verbinde mit LocalDB via Treiber: {driver}")

    # LocalDB starten (nur auf Windows ohne expliziten MSSQL_SERVER)
    if not os.environ.get("MSSQL_SERVER"):
        try:
            result = _subprocess.run(
                ["SqlLocalDB", "start", "MSSQLLocalDB"],
                capture_output=True, text=True, timeout=15
            )
            log(f"LocalDB: {result.stdout.strip() or result.stderr.strip() or 'gestartet'}")
        except Exception as e:
            log(f"LocalDB-Start übersprungen ({e})")

    # Temporäre Kopie erstellen (Original bleibt unberührt)
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

    # Vorherige gleichnamige DB ggf. detachen
    master_conn = pyodbc.connect(_build_conn_str(driver), autocommit=True)
    cur = master_conn.cursor()
    cur.execute("SELECT name FROM sys.databases WHERE name = ?", db_name)
    if cur.fetchone():
        log(f"Detache vorhandene DB [{db_name}] …")
        cur.execute(f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        cur.execute(f"EXEC sp_detach_db '{db_name}', 'true'")

    # Kopie anhängen
    mdf_win = _win_path(tmp_mdf).replace("'", "''")
    if tmp_ldf:
        ldf_win = _win_path(tmp_ldf).replace("'", "''")
        log("Hänge Kopie an (MDF + LDF) …")
        sql_attach = (
            f"CREATE DATABASE [{db_name}] ON "
            f"(FILENAME='{mdf_win}'), "
            f"(FILENAME='{ldf_win}') "
            f"FOR ATTACH"
        )
        sql_rebuild = (
            f"CREATE DATABASE [{db_name}] ON "
            f"(FILENAME='{mdf_win}') "
            f"FOR ATTACH_REBUILD_LOG"
        )
        try:
            cur.execute(sql_attach)
        except Exception as e:
            log(f"FOR ATTACH fehlgeschlagen ({e}) – versuche ATTACH_REBUILD_LOG …")
            # Re-check and detach if the failed attempt left a partial entry
            cur2 = master_conn.cursor()
            cur2.execute("SELECT name FROM sys.databases WHERE name = ?", db_name)
            if cur2.fetchone():
                cur2.execute(
                    f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                )
                cur2.execute(f"EXEC sp_detach_db '{db_name}', 'true'")
            cur.execute(sql_rebuild)
    else:
        log("Hänge Kopie an (nur MDF, Log wird neu erstellt) …")
        cur.execute(
            f"CREATE DATABASE [{db_name}] ON "
            f"(FILENAME='{mdf_win}') "
            f"FOR ATTACH_REBUILD_LOG"
        )

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
        mc  = pyodbc.connect(_build_conn_str(session.driver), autocommit=True)
        cur = mc.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE name = ?", session.db_name)
        if cur.fetchone():
            cur.execute(
                f"ALTER DATABASE [{session.db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            )
            cur.execute(f"EXEC sp_detach_db '{session.db_name}', 'true'")
            log(f"Temporäre DB [{session.db_name}] detacht.")
        else:
            log(f"Temporäre DB [{session.db_name}] war nicht angehängt – kein Detach nötig.")
        mc.close()
    except Exception as e:
        log(f"Detach-Warnung: {e}")
    if os.path.isdir(session.tmp_dir):
        try:
            shutil.rmtree(session.tmp_dir)
            log(f"Temp-Kopien gelöscht: {session.tmp_dir}")
        except Exception as e:
            log(f"Temp-Löschen fehlgeschlagen: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  Schema lesen
# ════════════════════════════════════════════════════════════════════════════
def read_schema(session: MdfSession, log) -> dict:
    """Liest Tabellen, Spalten, PKs, FKs und Views aus SQL Server."""
    cur    = session.cursor()
    schema = {"tables": {}, "views": {}}

    # ── Tabellen & Spalten ────────────────────────────────────────────────
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
            -- Fuer datetime2/time/datetimeoffset ist NUMERIC_SCALE NULL -
            -- die Nachkommastellen (fsp) stehen in DATETIME_PRECISION.
            COALESCE(c.NUMERIC_SCALE, c.DATETIME_PRECISION) AS NUMERIC_SCALE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA+'.'+t.TABLE_NAME),
                           c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY,
            COLUMNPROPERTY(OBJECT_ID(t.TABLE_SCHEMA+'.'+t.TABLE_NAME),
                           c.COLUMN_NAME, 'IsComputed') AS IS_COMPUTED
        FROM INFORMATION_SCHEMA.TABLES  t
        JOIN INFORMATION_SCHEMA.COLUMNS c
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
           AND c.TABLE_NAME   = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
    """)
    for row in cur.fetchall():
        tschema, tname, col, pos, nullable, dtype, maxlen, prec, scale, default, is_id, is_comp = row
        key = f"{tschema}.{tname}"
        if key not in schema["tables"]:
            schema["tables"][key] = {
                "schema": tschema, "name": tname,
                "columns": [], "pk": [], "fk": [],
            }
        schema["tables"][key]["columns"].append({
            "name": col, "pos": pos, "nullable": nullable == "YES",
            "type": dtype, "max_len": maxlen, "precision": prec, "scale": scale,
            "default": default, "identity": bool(is_id),
            "computed": bool(is_comp),
        })

    # ── Primary Keys ──────────────────────────────────────────────────────
    log("Lese Primary Keys …")
    cur.execute("""
        SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS  tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE   kcu
            ON  kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, kcu.ORDINAL_POSITION
    """)
    for tschema, tname, col in cur.fetchall():
        key = f"{tschema}.{tname}"
        if key in schema["tables"]:
            schema["tables"][key]["pk"].append(col)

    # ── Foreign Keys ──────────────────────────────────────────────────────
    log("Lese Foreign Keys …")
    cur.execute("""
        SELECT
            fk.name                                                        AS fk_name,
            OBJECT_SCHEMA_NAME(fkc.parent_object_id)                      AS from_schema,
            OBJECT_NAME(fkc.parent_object_id)                             AS from_table,
            COL_NAME(fkc.parent_object_id,    fkc.parent_column_id)       AS from_col,
            OBJECT_SCHEMA_NAME(fkc.referenced_object_id)                  AS to_schema,
            OBJECT_NAME(fkc.referenced_object_id)                         AS to_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id)  AS to_col,
            fk.delete_referential_action_desc                             AS on_delete,
            fk.update_referential_action_desc                             AS on_update
        FROM sys.foreign_keys        fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        ORDER BY from_schema, from_table, fk.name, fkc.constraint_column_id
    """)
    for row in cur.fetchall():
        fk_name, fs, ft, fc, ts, tt, tc, on_del, on_upd = row
        key = f"{fs}.{ft}"
        if key not in schema["tables"]:
            continue
        # Composite-FKs: Spalten pro Constraint-Name gruppieren - ein
        # Eintrag pro Spaltenpaar ergaebe zwei ADD CONSTRAINT mit
        # identischem Namen (zweiter schlaegt fehl).
        fk_list  = schema["tables"][key]["fk"]
        fk_entry = next((f for f in fk_list if f["name"] == fk_name), None)
        if fk_entry is None:
            fk_entry = {
                "name": fk_name,
                "from_cols": [], "to_cols": [],
                "to_schema": ts, "to_table": tt,
                "on_delete": on_del or "NO_ACTION",
                "on_update": on_upd or "NO_ACTION",
            }
            fk_list.append(fk_entry)
        fk_entry["from_cols"].append(fc)
        fk_entry["to_cols"].append(tc)

    # ── Indexes (UNIQUE + Non-Clustered, ohne PKs) ────────────────────────
    # Erfasst auch table-level UNIQUE CONSTRAINTs (is_unique_constraint=1) -
    # fuer MySQL ist eine UNIQUE CONSTRAINT identisch zu einem UNIQUE INDEX.
    log("Lese Indexes …")
    cur.execute("""
        SELECT
            OBJECT_SCHEMA_NAME(i.object_id)          AS tschema,
            OBJECT_NAME(i.object_id)                  AS tname,
            i.name                                    AS iname,
            i.is_unique,
            ic.key_ordinal,
            COL_NAME(ic.object_id, ic.column_id)      AS col_name,
            ic.is_descending_key,
            i.has_filter,
            i.filter_definition
        FROM sys.indexes        i
        JOIN sys.index_columns  ic ON ic.object_id = i.object_id
                                   AND ic.index_id  = i.index_id
        WHERE i.type              = 2              -- 2 = NONCLUSTERED
          AND i.is_primary_key    = 0
          AND ic.is_included_column  = 0           -- nur Schlüsselspalten
          AND OBJECT_SCHEMA_NAME(i.object_id) != 'sys'
        ORDER BY tschema, tname, iname, ic.key_ordinal
    """)
    for row in cur.fetchall():
        tschema, tname, iname, is_unique, _, col_name, is_desc, has_filter, filter_def = row
        key = f"{tschema}.{tname}"
        if key not in schema["tables"]:
            continue
        indexes = schema["tables"][key].setdefault("indexes", [])
        # Index in Liste suchen oder neu anlegen
        idx_entry = next((x for x in indexes if x["name"] == iname), None)
        if idx_entry is None:
            idx_entry = {
                "name":       iname,
                "unique":     bool(is_unique),
                "columns":    [],
                "filter":     filter_def or None,
            }
            indexes.append(idx_entry)
        idx_entry["columns"].append({
            "name": col_name,
            "desc": bool(is_desc),
        })

    idx_total = sum(len(t.get("indexes", [])) for t in schema["tables"].values())
    log(f"Indexes gelesen: {idx_total}")

    # ── Views ─────────────────────────────────────────────────────────────
    # INFORMATION_SCHEMA.VIEWS.VIEW_DEFINITION ist auf 4000 Zeichen begrenzt.
    # sys.sql_modules.definition ist nvarchar(MAX) → liefert vollständigen Text.
    log("Lese View-Definitionen …")
    cur.execute("""
        SELECT
            OBJECT_SCHEMA_NAME(v.object_id) AS vschema,
            v.name                           AS vname,
            m.definition                     AS vdef
        FROM sys.views       v
        JOIN sys.sql_modules m ON m.object_id = v.object_id
        ORDER BY vschema, vname
    """)
    for vschema, vname, vdef in cur.fetchall():
        schema["views"][f"{vschema}.{vname}"] = {
            "schema": vschema, "name": vname, "definition": vdef or "",
        }

    log(f"Schema gelesen: {len(schema['tables'])} Tabellen, {len(schema['views'])} Views.")
    return schema

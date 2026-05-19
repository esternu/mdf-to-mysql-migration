"""
Datenmigration: SQL Server → MySQL.

Enthält vier reine, testbare Kernfunktionen:
  - get_table_list   : Tabellenliste aus SQL Server lesen
  - get_row_counts   : Zeilenzahlen aller Tabellen vorab ermitteln
  - iter_table_data  : Spalten + Zeilen einer Tabelle als Chunk-Generator
  - migrate_table    : Einzelne Tabelle chunk-weise in MySQL schreiben
  - migrate_all      : Alle Tabellen migrieren, Zusammenfassung zurückgeben

Die Orchestrierung (Verbindungsaufbau, Konfiguration, Logging in Datei)
übernimmt der Runner run_migrate_data.py im Projekt-Root.
"""
from __future__ import annotations

import threading
from typing import Callable, Dict, Generator, List, Optional, Tuple, Any

try:
    import mysql.connector
    MYSQL_OK = True
except ImportError:
    mysql = None          # type: ignore
    MYSQL_OK = False


# ── Typ-Aliase ────────────────────────────────────────────────────────────────
LogFn        = Callable[[str], None]
ProgressFn   = Callable[[str, int, int], None]   # (table, rows_done, rows_total)
Row          = Tuple[Any, ...]
TableEntry   = Tuple[str, str]                   # (schema, table_name)

CHUNK_SIZE   = 5_000   # Zeilen pro Batch


# ════════════════════════════════════════════════════════════════════════════
#  1) Tabellenliste aus SQL Server lesen
# ════════════════════════════════════════════════════════════════════════════
def get_table_list(session) -> List[TableEntry]:
    """Gibt eine sortierte Liste aller Basis-Tabellen zurück."""
    cur = session.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM   INFORMATION_SCHEMA.TABLES
        WHERE  TABLE_TYPE = 'BASE TABLE'
        ORDER  BY TABLE_SCHEMA, TABLE_NAME
    """)
    return [(row[0], row[1]) for row in cur.fetchall()]


# ════════════════════════════════════════════════════════════════════════════
#  2) Zeilenzahlen vorab ermitteln
# ════════════════════════════════════════════════════════════════════════════
def get_row_counts(session, tables: List[TableEntry]) -> Dict[str, int]:
    """Liest Zeilenzahlen aus sys.partitions (schnell, ohne COUNT(*)-Scan).

    Gibt ein Dict {table_name: row_count} zurück.
    Tabellen ohne Eintrag (z.B. leere Heap-Tabellen) erhalten den Wert 0.
    """
    cur = session.cursor()
    cur.execute("""
        SELECT
            OBJECT_SCHEMA_NAME(p.object_id) AS tschema,
            OBJECT_NAME(p.object_id)        AS tname,
            SUM(p.rows)                     AS row_count
        FROM sys.partitions p
        WHERE p.index_id IN (0, 1)   -- 0 = heap, 1 = clustered index
        GROUP BY p.object_id
    """)
    counts: Dict[str, int] = {}
    for tschema, tname, cnt in cur.fetchall():
        counts[tname] = int(cnt or 0)

    # Tabellen die nicht in sys.partitions auftauchen → 0
    for _, tname in tables:
        counts.setdefault(tname, 0)
    return counts


# ════════════════════════════════════════════════════════════════════════════
#  3) Spalten + Zeilen einer Tabelle als Chunk-Generator
# ════════════════════════════════════════════════════════════════════════════
def iter_table_data(
    session,
    schema:     str,
    table:      str,
    chunk_size: int = CHUNK_SIZE,
    stop_event: Optional[threading.Event] = None,
) -> Generator[Tuple[List[str], List[Row]], None, None]:
    """Liest Spalten und Zeilen einer Tabelle in Chunks.

    Yields
    ------
    (columns, rows)
        columns – Spaltennamen (nur beim ersten Yield gefüllt, danach gleich)
        rows    – Liste von bis zu chunk_size Tupeln
    """
    cur = session.cursor()

    # Spaltennamen
    cur.execute("""
        SELECT COLUMN_NAME
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER  BY ORDINAL_POSITION
    """, schema, table)
    columns = [row[0] for row in cur.fetchall()]

    if not columns:
        return

    # Daten offset-basiert lesen (SQL Server 2012+)
    offset = 0
    while True:
        if stop_event and stop_event.is_set():
            return

        cur.execute(
            f"SELECT * FROM [{schema}].[{table}]"
            f" ORDER BY (SELECT NULL)"
            f" OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
        )
        rows = cur.fetchall()
        if not rows:
            return
        yield columns, list(rows)
        offset += len(rows)
        if len(rows) < chunk_size:
            return


# ════════════════════════════════════════════════════════════════════════════
#  4) Einzelne Tabelle chunk-weise in MySQL schreiben
# ════════════════════════════════════════════════════════════════════════════
def migrate_table(
    mysql_conn,
    table_name:        str,
    session,
    schema:            str,
    row_count:         int,
    log:               LogFn,
    chunk_size:        int = CHUNK_SIZE,
    progress_callback: Optional[ProgressFn] = None,
    stop_event:        Optional[threading.Event] = None,
) -> int:
    """Leert die Zieltabelle und schreibt alle Zeilen chunk-weise.

    Returns
    -------
    int  – Anzahl importierter Zeilen (0 wenn leer oder abgebrochen).
    """
    cur = mysql_conn.cursor()
    cur.execute(f"TRUNCATE TABLE `{table_name}`")
    mysql_conn.commit()

    rows_done   = 0
    insert_sql  = None
    first_chunk = True

    for columns, rows in iter_table_data(session, schema, table_name, chunk_size, stop_event):
        if stop_event and stop_event.is_set():
            log(f"  {table_name}: abgebrochen nach {rows_done} Zeilen")
            return rows_done

        if first_chunk:
            if not rows:
                log(f"  {table_name}: leer – übersprungen")
                return 0
            col_list    = ", ".join(f"`{c}`" for c in columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql  = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"
            first_chunk = False

        batch = [
            tuple(val if not isinstance(val, memoryview) else bytes(val) for val in row)
            for row in rows
        ]
        cur.executemany(insert_sql, batch)
        mysql_conn.commit()
        rows_done += len(rows)

        if progress_callback:
            progress_callback(table_name, rows_done, row_count)

    cur.close()

    if rows_done == 0:
        log(f"  {table_name}: leer – übersprungen")
    else:
        log(f"  {table_name}: {rows_done} Zeilen importiert ✓")
    return rows_done


# ════════════════════════════════════════════════════════════════════════════
#  5) Alle Tabellen migrieren
# ════════════════════════════════════════════════════════════════════════════
def migrate_all(
    session,
    mysql_conn,
    tables:            List[TableEntry],
    log:               LogFn,
    chunk_size:        int = CHUNK_SIZE,
    progress_callback: Optional[ProgressFn] = None,
    stop_event:        Optional[threading.Event] = None,
) -> dict:
    """Migriert alle übergebenen Tabellen von SQL Server nach MySQL.

    Returns
    -------
    dict mit Schlüsseln:
        "total_rows"   – Gesamtzahl importierter Zeilen (int)
        "skipped"      – Namen leerer Tabellen (list of str)
        "errors"       – Fehlermeldungen (list of str)
        "migrated"     – Zeilenzahl pro Tabelle {table: count} (dict)
        "cancelled"    – True wenn durch stop_event abgebrochen (bool)
    """
    result = {
        "total_rows": 0,
        "skipped":    [],
        "errors":     [],
        "migrated":   {},
        "cancelled":  False,
    }

    row_counts = get_row_counts(session, tables)

    mysql_conn.cursor().execute("SET FOREIGN_KEY_CHECKS = 0")
    mysql_conn.commit()

    for idx, (schema, tname) in enumerate(tables, 1):
        if stop_event and stop_event.is_set():
            result["cancelled"] = True
            log(f"⚠ Migration abgebrochen nach {idx - 1}/{len(tables)} Tabellen.")
            break

        log(f"  [{idx}/{len(tables)}] {tname} (~{row_counts.get(tname, 0)} Zeilen) …")

        try:
            count = migrate_table(
                mysql_conn, tname, session, schema,
                row_counts.get(tname, 0),
                log, chunk_size, progress_callback, stop_event,
            )
            if count == 0:
                result["skipped"].append(tname)
            else:
                result["migrated"][tname] = count
                result["total_rows"] += count
        except Exception as exc:
            msg = f"{tname}: {exc}"
            result["errors"].append(msg)
            log(f"  {tname}: FEHLER – {exc}")
            try:
                mysql_conn.rollback()
            except Exception:
                pass

    mysql_conn.cursor().execute("SET FOREIGN_KEY_CHECKS = 1")
    mysql_conn.commit()

    return result

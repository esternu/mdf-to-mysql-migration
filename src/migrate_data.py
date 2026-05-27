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

import datetime
import json
import os
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
#  5) Checkpoint-Hilfsfunktionen
# ════════════════════════════════════════════════════════════════════════════
def load_checkpoint(checkpoint_file: str) -> Dict[str, object]:
    """Liest Checkpoint-Datei; gibt leeres Dict zurück wenn nicht vorhanden."""
    if checkpoint_file and os.path.isfile(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            pass
    return {"completed": [], "started_at": None}


def save_checkpoint(checkpoint_file: str, completed: List[str], started_at: str) -> None:
    """Schreibt Checkpoint-Datei atomar."""
    if not checkpoint_file:
        return
    data = {"completed": completed, "started_at": started_at}
    tmp  = checkpoint_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, checkpoint_file)


def delete_checkpoint(checkpoint_file: str) -> None:
    """Löscht Checkpoint-Datei nach erfolgreichem Abschluss."""
    if checkpoint_file and os.path.isfile(checkpoint_file):
        try:
            os.remove(checkpoint_file)
        except Exception:
            pass


def checkpoint_exists(checkpoint_file: str) -> bool:
    """Gibt True zurück wenn eine Checkpoint-Datei mit abgeschlossenen Tabellen existiert."""
    cp = load_checkpoint(checkpoint_file)
    return bool(cp.get("completed"))


# ════════════════════════════════════════════════════════════════════════════
#  6) Alle Tabellen migrieren (mit Dry-Run und Checkpoint)
# ════════════════════════════════════════════════════════════════════════════
def migrate_all(
    session,
    mysql_conn,
    tables:            List[TableEntry],
    log:               LogFn,
    chunk_size:        int = CHUNK_SIZE,
    progress_callback: Optional[ProgressFn] = None,
    stop_event:        Optional[threading.Event] = None,
    dry_run:           bool = False,
    checkpoint_file:   Optional[str] = None,
) -> dict:
    """Migriert alle übergebenen Tabellen von SQL Server nach MySQL.

    Parameters
    ----------
    dry_run : bool
        Wenn True: Zeilenzahlen loggen, aber kein TRUNCATE/INSERT ausführen.
    checkpoint_file : str | None
        Pfad zur Checkpoint-JSON-Datei. Bereits migrierte Tabellen werden
        übersprungen; nach jeder Tabelle wird der Fortschritt gespeichert.
        Nach erfolgreichem Abschluss wird die Datei gelöscht.

    Returns
    -------
    dict mit Schlüsseln:
        "total_rows"   – Gesamtzahl importierter Zeilen (int)
        "skipped"      – Namen leerer/übersprungener Tabellen (list of str)
        "errors"       – Fehlermeldungen (list of str)
        "migrated"     – Zeilenzahl pro Tabelle {table: count} (dict)
        "cancelled"    – True wenn durch stop_event abgebrochen (bool)
        "dry_run"      – True wenn Dry-Run-Modus aktiv war (bool)
    """
    result = {
        "total_rows": 0,
        "skipped":    [],
        "errors":     [],
        "migrated":   {},
        "cancelled":  False,
        "dry_run":    dry_run,
    }

    row_counts  = get_row_counts(session, tables)
    started_at  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Checkpoint laden ──────────────────────────────────────────────────
    cp          = load_checkpoint(checkpoint_file)
    completed   = list(cp.get("completed") or [])
    if completed:
        log(f"  Checkpoint: {len(completed)} Tabellen bereits migriert – werden übersprungen.")

    # ── Dry-Run: nur Übersicht loggen ─────────────────────────────────────
    if dry_run:
        log("── Dry-Run: kein Schreiben, nur Vorschau ──")
        total_rows = 0
        for schema, tname in tables:
            cnt = row_counts.get(tname, 0)
            total_rows += cnt
            status = "✓ bereits migriert" if tname in completed else f"~{cnt:,} Zeilen"
            log(f"  {tname}: {status}")
        log(f"── Gesamt: {len(tables)} Tabellen, ~{total_rows:,} Zeilen ──")
        return result

    # ── Echte Migration ───────────────────────────────────────────────────
    mysql_conn.cursor().execute("SET FOREIGN_KEY_CHECKS = 0")
    mysql_conn.commit()

    for idx, (schema, tname) in enumerate(tables, 1):
        if stop_event and stop_event.is_set():
            result["cancelled"] = True
            log(f"⚠ Migration abgebrochen nach {idx - 1}/{len(tables)} Tabellen.")
            break

        # Bereits abgeschlossene Tabellen überspringen
        if tname in completed:
            log(f"  [{idx}/{len(tables)}] {tname}: übersprungen (Checkpoint) ✓")
            result["skipped"].append(tname)
            continue

        log(f"  [{idx}/{len(tables)}] {tname} (~{row_counts.get(tname, 0):,} Zeilen) …")

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

            # Checkpoint aktualisieren (auch leere Tabellen als abgeschlossen markieren)
            completed.append(tname)
            save_checkpoint(checkpoint_file, completed, started_at)

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

    # Checkpoint löschen wenn alles fehlerfrei abgeschlossen
    if not result["cancelled"] and not result["errors"]:
        delete_checkpoint(checkpoint_file)
        if checkpoint_file:
            log("  Checkpoint gelöscht (Migration vollständig).")

    return result

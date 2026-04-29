"""
Datenmigration: SQL Server → MySQL.

Enthält vier reine, testbare Kernfunktionen:
  - get_table_list   : Tabellenliste aus SQL Server lesen
  - read_table_data  : Spalten + Zeilen einer Tabelle lesen
  - migrate_table    : Einzelne Tabelle in MySQL schreiben
  - migrate_all      : Alle Tabellen migrieren, Zusammenfassung zurückgeben

Die Orchestrierung (Verbindungsaufbau, Konfiguration, Logging in Datei)
übernimmt der Runner run_migrate_data.py im Projekt-Root.
"""
from __future__ import annotations

from typing import Callable, List, Tuple, Any

# mysql.connector wird nur zur Laufzeit benötigt; optionaler Import
# ermöglicht Unit-Tests ohne installiertes mysql-connector-python.
try:
    import mysql.connector
    MYSQL_OK = True
except ImportError:
    mysql = None          # type: ignore
    MYSQL_OK = False


# ── Typ-Aliase ────────────────────────────────────────────────────────────────
LogFn      = Callable[[str], None]
Row        = Tuple[Any, ...]
TableEntry = Tuple[str, str]          # (schema, table_name)


# ════════════════════════════════════════════════════════════════════════════
#  1) Tabellenliste aus SQL Server lesen
# ════════════════════════════════════════════════════════════════════════════
def get_table_list(session) -> List[TableEntry]:
    """Gibt eine sortierte Liste aller Basis-Tabellen zurück.

    Parameters
    ----------
    session : MdfSession
        Aktive pyodbc-Session gegen den SQL Server.

    Returns
    -------
    list of (schema, table_name) tuples, alphabetisch sortiert.
    """
    cur = session.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM   INFORMATION_SCHEMA.TABLES
        WHERE  TABLE_TYPE = 'BASE TABLE'
        ORDER  BY TABLE_SCHEMA, TABLE_NAME
    """)
    return [(row[0], row[1]) for row in cur.fetchall()]


# ════════════════════════════════════════════════════════════════════════════
#  2) Spalten + Zeilen einer Tabelle lesen
# ════════════════════════════════════════════════════════════════════════════
def read_table_data(session, schema: str, table: str) -> Tuple[List[str], List[Row]]:
    """Liest alle Spalten und Zeilen einer SQL-Server-Tabelle.

    Parameters
    ----------
    session : MdfSession
        Aktive pyodbc-Session.
    schema : str
        SQL-Server-Schema (z. B. "dbo").
    table : str
        Tabellenname (z. B. "TableArticle").

    Returns
    -------
    (columns, rows)
        columns – Spaltennamen in ORDINAL_POSITION-Reihenfolge
        rows    – Liste von Tupeln (eine pro Zeile)
    """
    cur = session.cursor()

    # Spaltennamen in definierter Reihenfolge
    cur.execute("""
        SELECT COLUMN_NAME
        FROM   INFORMATION_SCHEMA.COLUMNS
        WHERE  TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER  BY ORDINAL_POSITION
    """, schema, table)
    columns = [row[0] for row in cur.fetchall()]

    # Alle Zeilen
    cur.execute(f"SELECT * FROM [{schema}].[{table}]")
    rows = cur.fetchall()

    return columns, list(rows)


# ════════════════════════════════════════════════════════════════════════════
#  3) Einzelne Tabelle in MySQL schreiben
# ════════════════════════════════════════════════════════════════════════════
def migrate_table(
    mysql_conn,
    table_name: str,
    columns:    List[str],
    rows:       List[Row],
    log:        LogFn,
) -> int:
    """Leert die Zieltabelle und schreibt alle übergebenen Zeilen.

    Parameters
    ----------
    mysql_conn : mysql.connector.connection
        Offene MySQL-Verbindung.
    table_name : str
        Name der Zieltabelle in MySQL.
    columns : list of str
        Spaltennamen (müssen in Zieltabelle existieren).
    rows : list of tuple
        Zu importierende Zeilen.
    log : callable
        Einzeiliger Log-Callback.

    Returns
    -------
    int
        Anzahl der importierten Zeilen.

    Raises
    ------
    Exception
        Bei MySQL-Fehlern wird die Ausnahme weitergeleitet;
        der Aufrufer ist für Rollback und Logging verantwortlich.
    """
    if not rows:
        log(f"  {table_name}: leer – übersprungen")
        return 0

    cur = mysql_conn.cursor()

    # Zieltabelle leeren
    cur.execute(f"TRUNCATE TABLE `{table_name}`")

    # INSERT vorbereiten
    col_list     = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql   = f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})"

    # bytes-Werte direkt übergeben, Rest unverändert
    batch = [
        tuple(val if not isinstance(val, memoryview) else bytes(val) for val in row)
        for row in rows
    ]

    cur.executemany(insert_sql, batch)
    mysql_conn.commit()
    cur.close()

    log(f"  {table_name}: {len(rows)} Zeilen importiert ✓")
    return len(rows)


# ════════════════════════════════════════════════════════════════════════════
#  4) Alle Tabellen migrieren
# ════════════════════════════════════════════════════════════════════════════
def migrate_all(
    session,
    mysql_conn,
    tables: List[TableEntry],
    log:    LogFn,
) -> dict:
    """Migriert alle übergebenen Tabellen von SQL Server nach MySQL.

    Parameters
    ----------
    session : MdfSession
        Aktive SQL-Server-Session.
    mysql_conn : mysql.connector.connection
        Offene MySQL-Verbindung.
    tables : list of (schema, table_name)
        Zu migrierende Tabellen.
    log : callable
        Einzeiliger Log-Callback.

    Returns
    -------
    dict mit Schlüsseln:
        "total_rows"   – Gesamtzahl importierter Zeilen (int)
        "skipped"      – Namen leerer Tabellen (list of str)
        "errors"       – Fehlermeldungen (list of str)
        "migrated"     – Zeilenzahl pro Tabelle {table: count} (dict)
    """
    result = {
        "total_rows": 0,
        "skipped":    [],
        "errors":     [],
        "migrated":   {},
    }

    mysql_conn.cursor().execute("SET FOREIGN_KEY_CHECKS = 0")
    mysql_conn.commit()

    for schema, tname in tables:
        try:
            columns, rows = read_table_data(session, schema, tname)
            count = migrate_table(mysql_conn, tname, columns, rows, log)
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

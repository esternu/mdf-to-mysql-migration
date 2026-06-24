"""
MySQL Deployment.
Verbindet mit dem Synology-MySQL-Server und führt das generierte DDL aus.
"""
from typing import Callable, List, Optional

try:
    import mysql.connector
    MYSQL_OK = True
except ImportError:
    mysql    = None   # type: ignore
    MYSQL_OK = False


def _split_statements(ddl: str) -> List[str]:
    """Zerlegt DDL in Einzelanweisungen.

    Semikolons innerhalb von -- Kommentaren oder String-Literalen ('...') werden
    als Trennzeichen ignoriert.  Einfache ddl.split(';') würde sonst
    Kommentare wie  -- decimal(18,x) waere ungueltig;  fälschlich aufteilen.
    """
    statements: List[str] = []
    buf: List[str] = []
    in_string = False
    i = 0
    n = len(ddl)

    while i < n:
        c = ddl[i]

        if in_string:
            buf.append(c)
            if c == "'":
                # Escaped quote '' bleibt im String
                if i + 1 < n and ddl[i + 1] == "'":
                    buf.append(ddl[i + 1])
                    i += 2
                    continue
                else:
                    in_string = False
            i += 1
            continue

        if c == "'":
            in_string = True
            buf.append(c)
            i += 1
            continue

        # Einzeiliger Kommentar: -- bis Zeilenende überspringen
        if c == '-' and i + 1 < n and ddl[i + 1] == '-':
            end = ddl.find('\n', i)
            if end == -1:
                end = n - 1
            buf.append(ddl[i:end + 1])
            i = end + 1
            continue

        # Mehrzeiliger Kommentar: /* ... */
        if c == '/' and i + 1 < n and ddl[i + 1] == '*':
            end = ddl.find('*/', i + 2)
            if end == -1:
                end = n - 2
            buf.append(ddl[i:end + 2])
            i = end + 2
            continue

        if c == ';':
            stmt = ''.join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(c)
        i += 1

    remaining = ''.join(buf).strip()
    if remaining:
        statements.append(remaining)

    return statements


def deploy_to_mysql(
    ddl: str,
    host: str,
    port: int,
    user: str,
    password: str,
    target_db: str,
    log: Callable[[str], None],
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> None:
    """Führt das übergebene DDL-Script auf dem MySQL-Server aus.

    Jede durch ';' getrennte Anweisung wird einzeln ausgeführt.
    Semikolons in -- Kommentaren und String-Literalen werden ignoriert.
    Fehler werden gesammelt und am Ende als Block geloggt, damit
    der Rest des Scripts trotzdem durchläuft. Gab es mindestens einen
    Fehler, wird am Ende ein RuntimeError geworfen, damit der Aufrufer
    keinen Erfolg meldet, obwohl Anweisungen fehlgeschlagen sind.

    progress_callback(done, total) wird nach jeder ausgeführten Anweisung aufgerufen.
    """
    log(f"Verbinde mit MySQL {host}:{port} …")
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=target_db,
        allow_local_infile=True,
        charset="utf8mb4",
        connection_timeout=10,
    )
    cur = conn.cursor()

    statements = _split_statements(ddl)
    total      = len(statements)
    log(f"{total} SQL-Anweisungen werden ausgeführt …")

    errors = []
    for i, stmt in enumerate(statements, 1):
        try:
            cur.execute(stmt)
            conn.commit()
        except mysql.connector.Error as e:
            errors.append(f"[{i}/{total}] {e}\n  SQL: {stmt[:120]}")
        if progress_callback:
            progress_callback(i, total)

    cur.close()
    conn.close()

    if errors:
        log(f"\n⚠ {len(errors)} Fehler aufgetreten:")
        for err in errors:
            log("  " + err)
        raise RuntimeError(f"{len(errors)} von {total} SQL-Anweisungen fehlgeschlagen")
    else:
        log(f"\n✓ Alle {total} Anweisungen erfolgreich ausgeführt.")

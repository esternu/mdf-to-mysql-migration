"""
MySQL Deployment.
Verbindet mit dem Synology-MySQL-Server und führt das generierte DDL aus.
"""
from typing import Callable

try:
    import mysql.connector
    MYSQL_OK = True
except ImportError:
    mysql    = None   # type: ignore
    MYSQL_OK = False


def deploy_to_mysql(
    ddl: str,
    host: str,
    port: int,
    user: str,
    password: str,
    target_db: str,
    log: Callable[[str], None],
) -> None:
    """Führt das übergebene DDL-Script auf dem MySQL-Server aus.

    Jede durch ';' getrennte Anweisung wird einzeln ausgeführt.
    Fehler werden gesammelt und am Ende als Block geloggt, damit
    der Rest des Scripts trotzdem durchläuft.
    """
    log(f"Verbinde mit MySQL {host}:{port} …")
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        allow_local_infile=True,
        charset="utf8mb4",
        connection_timeout=10,
    )
    cur = conn.cursor()

    statements = [s.strip() for s in ddl.split(";") if s.strip()]
    total      = len(statements)
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

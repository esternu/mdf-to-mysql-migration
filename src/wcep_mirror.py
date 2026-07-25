"""
Spiegelung des generierten Schemas/Audit-Triggers in den kanonischen
WCEP-Schema-Ordner (Sibling-Repo). Siehe WCEP/Tools/schema/README.md:
Cockpit_DatenBank.sql soll NICHT von Hand gepflegt, sondern vom
mdf-to-mysql-migration-Tool bei jedem DDL-Lauf geschrieben werden.

Als reine Funktion gehalten, damit sie ohne tkinter testbar ist.
"""
import os
from typing import Callable, List, Optional

from paths import WCEP_SCHEMA_DIR, WCEP_SCHEMA_FILENAME

# Nur für diese Zieldatenbank wird gespiegelt (die produktive WCEP-DB).
WCEP_TARGET_DB = "Cockpit_Datenbank"


def mirror_to_wcep_schema(
    target_db:      str,
    ddl:            str,
    audit_sql:      str,
    audit_filename: str,
    log:            Optional[Callable[[str], None]] = None,
    schema_dir:     str = WCEP_SCHEMA_DIR,
) -> List[str]:
    """Schreibt Schema-DDL (als Cockpit_DatenBank.sql) und Audit-Trigger in
    den kanonischen WCEP-Schema-Ordner – aber nur, wenn:

      * target_db == "Cockpit_Datenbank" (die produktive WCEP-DB) und
      * der WCEP-Schema-Ordner existiert (Sibling-Repo ist ausgecheckt).

    Returns
    -------
    Liste der tatsächlich geschriebenen Pfade (leer, wenn nichts gespiegelt
    wurde – z.B. andere Zieldatenbank oder WCEP-Repo nicht vorhanden).
    """
    if target_db != WCEP_TARGET_DB or not os.path.isdir(schema_dir):
        return []

    written: List[str] = []

    schema_path = os.path.join(schema_dir, WCEP_SCHEMA_FILENAME)
    with open(schema_path, "w", encoding="utf-8") as fh:
        fh.write(ddl)
    written.append(schema_path)
    if log:
        log(f"Schema-DDL → WCEP Schema: {schema_path}")

    audit_path = os.path.join(schema_dir, audit_filename)
    with open(audit_path, "w", encoding="utf-8") as fh:
        fh.write(audit_sql)
    written.append(audit_path)
    if log:
        log(f"Audit-Trigger → WCEP Schema: {audit_path}")

    return written

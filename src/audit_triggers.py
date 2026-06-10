"""
MySQL Audit-Trigger-Generator (P4).

Aequivalent zu Cockpit_Databases/Triggers/TriggerTableAudit.sql +
dbo.TableAuditLog + dbo.ViewAuditChanges, aber fuer MySQL/MariaDB.

CREATE TRIGGER ist in MySQL nicht in Prepared Statements erlaubt, daher
erzeugt dieses Modul (statt eines serverseitigen Cursor-Scripts) ein
fertiges SQL-Skript aus dem gelesenen Schema-Dict:

  - CREATE TABLE TableAuditLog (Id, TableName, Action, ChangedAt,
    ChangedBy, OldValue JSON, NewValue JSON)
  - pro Tabelle 3 Row-Trigger (AFTER INSERT/UPDATE/DELETE) mit
    JSON_OBJECT() statt FOR JSON PATH
  - UPDATE-Trigger speichert nur tatsaechlich geaenderte Spalten
    (ersetzt ViewAuditChanges, vgl. P4.3) plus die PK-Spalten zur
    Identifikation der betroffenen Zeile
  - ChangedBy wird aus einer Editor-Spalte uebernommen, falls vorhanden

Das Skript verwendet `DELIMITER $$` (mysql-CLI-Syntax) und ist fuer die
manuelle Ausfuehrung gedacht (P4: "nicht uebersetzbar, hand-geschrieben").
"""
from typing import Iterable, Optional

from .transform import mssql_name

DEFAULT_EXCLUDED_TABLES = {"tableauditlog", "row_locks", "databaseversion"}

AUDIT_LOG_TABLE_DDL = """DROP TABLE IF EXISTS `TableAuditLog`$$
CREATE TABLE `TableAuditLog` (
  `Id`        BIGINT NOT NULL AUTO_INCREMENT,
  `TableName` VARCHAR(150) NOT NULL,
  `Action`    VARCHAR(10) NOT NULL,
  `ChangedAt` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `ChangedBy` VARCHAR(150) NULL,
  `OldValue`  JSON NULL,
  `NewValue`  JSON NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci$$

CREATE INDEX `IX_TableAuditLog_ChangedAt` ON `TableAuditLog` (`ChangedAt`)$$
CREATE INDEX `IX_TableAuditLog_TableName` ON `TableAuditLog` (`TableName`)$$
CREATE INDEX `IX_TableAuditLog_ChangedBy` ON `TableAuditLog` (`ChangedBy`)$$"""


def _has_editor(columns: list) -> bool:
    return any(c["name"].lower() == "editor" for c in columns)


def _json_object(columns: list, alias: str) -> str:
    parts = ", ".join(
        f"'{c['name']}', {alias}.{mssql_name(c['name'])}" for c in columns
    )
    return f"JSON_OBJECT({parts})"


def _insert_trigger(tname: str, columns: list, has_editor: bool) -> str:
    changed_by = "NEW.`Editor`" if has_editor else "NULL"
    new_json   = _json_object(columns, "NEW")
    return (
        f"DROP TRIGGER IF EXISTS `trg_{tname}_Audit_Insert`$$\n"
        f"CREATE TRIGGER `trg_{tname}_Audit_Insert`\n"
        f"AFTER INSERT ON {mssql_name(tname)}\n"
        f"FOR EACH ROW\n"
        f"BEGIN\n"
        f"  INSERT INTO `TableAuditLog` (`TableName`, `Action`, `ChangedBy`, `NewValue`)\n"
        f"  VALUES ('{tname}', 'INSERT', {changed_by}, {new_json});\n"
        f"END$$"
    )


def _delete_trigger(tname: str, columns: list, has_editor: bool) -> str:
    changed_by = "OLD.`Editor`" if has_editor else "NULL"
    old_json   = _json_object(columns, "OLD")
    return (
        f"DROP TRIGGER IF EXISTS `trg_{tname}_Audit_Delete`$$\n"
        f"CREATE TRIGGER `trg_{tname}_Audit_Delete`\n"
        f"AFTER DELETE ON {mssql_name(tname)}\n"
        f"FOR EACH ROW\n"
        f"BEGIN\n"
        f"  INSERT INTO `TableAuditLog` (`TableName`, `Action`, `ChangedBy`, `OldValue`)\n"
        f"  VALUES ('{tname}', 'DELETE', {changed_by}, {old_json});\n"
        f"END$$"
    )


def _update_trigger(tname: str, columns: list, pk: list, has_editor: bool) -> str:
    changed_by = "NEW.`Editor`" if has_editor else "NULL"
    pk_set     = set(pk)
    other_cols = [c for c in columns if c["name"] not in pk_set]
    pk_cols    = [c for c in columns if c["name"] in pk_set]

    init_old = _json_object(pk_cols, "OLD") if pk_cols else "JSON_OBJECT()"
    init_new = _json_object(pk_cols, "NEW") if pk_cols else "JSON_OBJECT()"

    lines = [
        f"DROP TRIGGER IF EXISTS `trg_{tname}_Audit_Update`$$",
        f"CREATE TRIGGER `trg_{tname}_Audit_Update`",
        f"AFTER UPDATE ON {mssql_name(tname)}",
        "FOR EACH ROW",
        "BEGIN",
        f"  SET @audit_old = {init_old};",
        f"  SET @audit_new = {init_new};",
        "  SET @audit_changed = 0;",
    ]
    for c in other_cols:
        col = mssql_name(c["name"])
        lines.append(f"  IF NOT (OLD.{col} <=> NEW.{col}) THEN")
        lines.append(f"    SET @audit_old = JSON_SET(@audit_old, '$.{c['name']}', OLD.{col});")
        lines.append(f"    SET @audit_new = JSON_SET(@audit_new, '$.{c['name']}', NEW.{col});")
        lines.append("    SET @audit_changed = 1;")
        lines.append("  END IF;")
    lines.append("  IF @audit_changed = 1 THEN")
    lines.append("    INSERT INTO `TableAuditLog` (`TableName`, `Action`, `ChangedBy`, `OldValue`, `NewValue`)")
    lines.append(f"    VALUES ('{tname}', 'UPDATE', {changed_by}, @audit_old, @audit_new);")
    lines.append("  END IF;")
    lines.append("END$$")
    return "\n".join(lines)


def generate_audit_triggers(schema: dict, exclude_tables: Optional[Iterable[str]] = None) -> str:
    """Erzeugt ein SQL-Skript mit TableAuditLog + Audit-Triggern für alle
    Tabellen des Schema-Dicts (ausser den ausgeschlossenen).

    `exclude_tables` (case-insensitive) wird zusaetzlich zu
    DEFAULT_EXCLUDED_TABLES ausgeschlossen.
    """
    excluded = set(DEFAULT_EXCLUDED_TABLES)
    if exclude_tables:
        excluded |= {t.lower() for t in exclude_tables}

    lines = [
        "-- Generiert von mdf-to-mysql-migration: Audit-Trigger (P4)",
        "-- Manuelle Ausfuehrung via mysql-CLI (DELIMITER wird benoetigt)",
        "",
        "DELIMITER $$",
        "",
        AUDIT_LOG_TABLE_DDL,
        "",
    ]

    for tinfo in schema["tables"].values():
        tname = tinfo["name"]
        if tname.lower() in excluded:
            continue
        columns    = tinfo["columns"]
        pk         = tinfo.get("pk") or []
        has_editor = _has_editor(columns)

        lines.append(f"-- Tabelle: {tname}")
        lines.append(_insert_trigger(tname, columns, has_editor))
        lines.append("")
        lines.append(_update_trigger(tname, columns, pk, has_editor))
        lines.append("")
        lines.append(_delete_trigger(tname, columns, has_editor))
        lines.append("")

    lines.append("DELIMITER ;")
    return "\n".join(lines)

"""
Schema-Diff: MDF-Schema vs. bestehendes MySQL-Schema.

Vergleicht das aus der MDF gelesene Schema mit dem aktuellen MySQL-Schema
und generiert nur die nötigen inkrementellen Änderungen (ALTER TABLE,
CREATE TABLE, CREATE INDEX etc.).  Bestehende Daten bleiben erhalten.

Hauptfunktionen:
  read_mysql_schema   – aktuelles MySQL-Schema lesen
  diff_schemas        – Unterschiede ermitteln
  generate_diff_ddl   – DDL für die Änderungen erzeugen
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from transform import convert_type, mssql_name


# ════════════════════════════════════════════════════════════════════════════
#  MySQL-Schema lesen
# ════════════════════════════════════════════════════════════════════════════

def read_mysql_schema(conn, db_name: str) -> Dict[str, Any]:
    """Liest das aktuelle MySQL-Schema (Tabellen, Spalten, Indexes, FKs).

    Returns
    -------
    dict mit Schlüssel "tables":
        {
          table_name: {
              "columns":  {col_name: {"type": str, "nullable": bool, ...}},
              "pk":       [col_name, ...],
              "indexes":  {index_name: {"unique": bool, "columns": [col, ...]}},
              "fks":      {fk_name: {"from_col": str, "to_table": str, "to_col": str}},
          }
        }
    """
    cur = conn.cursor()
    schema: Dict[str, Any] = {"tables": {}}

    # ── Tabellen + Spalten (nur BASE TABLE, keine Views) ─────────────────
    cur.execute("""
        SELECT c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_TYPE, c.IS_NULLABLE,
               c.COLUMN_DEFAULT, c.EXTRA, c.ORDINAL_POSITION
        FROM   INFORMATION_SCHEMA.COLUMNS c
        JOIN   INFORMATION_SCHEMA.TABLES  t
               ON  t.TABLE_NAME   = c.TABLE_NAME
               AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE  c.TABLE_SCHEMA = %s
          AND  t.TABLE_TYPE   = 'BASE TABLE'
        ORDER  BY c.TABLE_NAME, c.ORDINAL_POSITION
    """, (db_name,))

    for tname, cname, ctype, nullable, default, extra, _ in cur.fetchall():
        if tname not in schema["tables"]:
            schema["tables"][tname] = {
                "columns": {},
                "pk":      [],
                "indexes": {},
                "fks":     {},
            }
        schema["tables"][tname]["columns"][cname] = {
            "type":        ctype.upper() if ctype else "",
            "nullable":    (nullable == "YES"),
            "default":     default,
            "auto_increment": ("auto_increment" in (extra or "").lower()),
        }

    # ── Primary Keys (nur BASE TABLE) ────────────────────────────────────
    cur.execute("""
        SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME
        FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN   INFORMATION_SCHEMA.TABLES t
               ON  t.TABLE_NAME   = kcu.TABLE_NAME
               AND t.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        WHERE  kcu.TABLE_SCHEMA    = %s
          AND  kcu.CONSTRAINT_NAME = 'PRIMARY'
          AND  t.TABLE_TYPE        = 'BASE TABLE'
        ORDER  BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION
    """, (db_name,))
    for tname, cname in cur.fetchall():
        if tname in schema["tables"]:
            schema["tables"][tname]["pk"].append(cname)

    # ── Indexes (ohne PK und FK, nur BASE TABLE) ─────────────────────────
    cur.execute("""
        SELECT s.TABLE_NAME, s.INDEX_NAME, s.NON_UNIQUE, s.COLUMN_NAME, s.SEQ_IN_INDEX
        FROM   INFORMATION_SCHEMA.STATISTICS s
        JOIN   INFORMATION_SCHEMA.TABLES t
               ON  t.TABLE_NAME   = s.TABLE_NAME
               AND t.TABLE_SCHEMA = s.TABLE_SCHEMA
        WHERE  s.TABLE_SCHEMA = %s
          AND  s.INDEX_NAME  != 'PRIMARY'
          AND  t.TABLE_TYPE   = 'BASE TABLE'
        ORDER  BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX
    """, (db_name,))
    for tname, iname, non_unique, cname, _ in cur.fetchall():
        if tname not in schema["tables"]:
            continue
        tbl = schema["tables"][tname]
        if iname not in tbl["indexes"]:
            tbl["indexes"][iname] = {"unique": not bool(non_unique), "columns": []}
        tbl["indexes"][iname]["columns"].append(cname)

    # ── Foreign Keys ──────────────────────────────────────────────────────
    cur.execute("""
        SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME,
               kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
        FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN   INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
               ON  tc.CONSTRAINT_NAME  = kcu.CONSTRAINT_NAME
               AND tc.TABLE_SCHEMA     = kcu.TABLE_SCHEMA
               AND tc.TABLE_NAME       = kcu.TABLE_NAME
        WHERE  kcu.TABLE_SCHEMA = %s
          AND  tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    """, (db_name,))
    for tname, fkname, col, ref_table, ref_col in cur.fetchall():
        if tname in schema["tables"]:
            schema["tables"][tname]["fks"][fkname] = {
                "from_col": col,
                "to_table": ref_table,
                "to_col":   ref_col,
            }

    cur.close()
    return schema


# ════════════════════════════════════════════════════════════════════════════
#  Typ-Normalisierung für Vergleich
# ════════════════════════════════════════════════════════════════════════════

def _normalize_mysql_type(t: str) -> str:
    """Normalisiert MySQL-Typangaben für stabilen Vergleich.

    Beispiele:
      'INT(11)'       → 'INT'
      'TINYINT(1)'    → 'TINYINT(1)'   ← behalten (=bool)
      'VARCHAR(255)'  → 'VARCHAR(255)'
      'DECIMAL(19,4)' → 'DECIMAL(19,4)'
    """
    t = t.strip().upper()
    # INT/BIGINT/SMALLINT/TINYINT ohne Länge normalisieren
    # Ausnahme: TINYINT(1) bleibt (entspricht BIT/bool)
    t = re.sub(r'\bINT\(\d+\)',     'INT',     t)
    t = re.sub(r'\bBIGINT\(\d+\)',  'BIGINT',  t)
    t = re.sub(r'\bSMALLINT\(\d+\)','SMALLINT',t)
    return t


def _source_mysql_type(col: dict) -> str:
    """Gibt den MySQL-Typ zurück den das MDF-Schema für diese Spalte vorsieht."""
    return _normalize_mysql_type(
        convert_type(col["type"], col.get("max_len"), col.get("precision"), col.get("scale"))
    )


# ════════════════════════════════════════════════════════════════════════════
#  Schemas vergleichen
# ════════════════════════════════════════════════════════════════════════════

def diff_schemas(source: dict, target_mysql: dict) -> dict:
    """Vergleicht MDF-Schema (source) mit bestehendem MySQL-Schema (target_mysql).

    Parameters
    ----------
    source        : Schema-Dict wie von mssql.read_schema() geliefert
    target_mysql  : Schema-Dict wie von read_mysql_schema() geliefert

    Returns
    -------
    diff-Dict:
    {
      "new_tables":     [tinfo, ...],             # komplett neu anlegen
      "altered_tables": {                         # bestehend, aber geändert
          table_name: {
              "new_columns":      [col_info, ...],
              "modified_columns": [(col_name, old_type, new_type), ...],
              "new_indexes":      [idx_info, ...],
              "new_fks":          [fk_info, ...],
          }
      },
      "removed_tables":  [name, ...],             # nur Warnung, kein Auto-Drop
      "removed_columns": {table: [col, ...]},     # nur Warnung
      "warnings":        [str, ...],
    }
    """
    diff: dict = {
        "new_tables":     [],
        "altered_tables": {},
        "removed_tables": [],
        "removed_columns": {},
        "warnings":       [],
    }

    src_tables = source.get("tables", {})
    tgt_tables = target_mysql.get("tables", {})

    src_names = {v["name"].lower(): v for v in src_tables.values()}
    tgt_names = {k.lower(): v for k, v in tgt_tables.items()}

    # ── Neue Tabellen ─────────────────────────────────────────────────────
    for lname, tinfo in src_names.items():
        if lname not in tgt_names:
            diff["new_tables"].append(tinfo)

    # ── Entfernte Tabellen (nur Warnung) ──────────────────────────────────
    for lname in tgt_names:
        if lname not in src_names:
            diff["removed_tables"].append(lname)
            diff["warnings"].append(
                f"Tabelle '{lname}' existiert in MySQL aber nicht mehr in MDF "
                f"– wird NICHT gelöscht (manuell prüfen)."
            )

    # ── Bestehende Tabellen vergleichen ───────────────────────────────────
    for lname, tinfo in src_names.items():
        if lname not in tgt_names:
            continue   # neue Tabelle, oben schon erfasst

        tgt = tgt_names[lname]
        tbl_name = tinfo["name"]
        changes: dict = {
            "new_columns":      [],
            "modified_columns": [],
            "new_indexes":      [],
            "new_fks":          [],
        }

        src_cols = {c["name"].lower(): c for c in tinfo["columns"]}
        tgt_cols = {k.lower(): v       for k, v in tgt["columns"].items()}

        # Neue + geänderte Spalten
        for cname_lower, col in src_cols.items():
            if cname_lower not in tgt_cols:
                changes["new_columns"].append(col)
            else:
                src_type = _normalize_mysql_type(_source_mysql_type(col))
                tgt_type = _normalize_mysql_type(tgt_cols[cname_lower]["type"])
                if src_type != tgt_type:
                    changes["modified_columns"].append(
                        (col["name"], tgt_type, src_type)
                    )
                    diff["warnings"].append(
                        f"{tbl_name}.{col['name']}: Typ ändert sich "
                        f"{tgt_type} → {src_type} – bestehende Daten prüfen!"
                    )

        # Entfernte Spalten (nur Warnung)
        removed = [k for k in tgt_cols if k not in src_cols]
        if removed:
            diff["removed_columns"][tbl_name] = [
                tgt["columns"][c] for c in tgt["columns"] if c.lower() in removed
            ]
            for c in removed:
                diff["warnings"].append(
                    f"{tbl_name}.{c}: Spalte existiert in MySQL aber nicht mehr in MDF "
                    f"– wird NICHT gelöscht."
                )

        # Neue Indexes
        src_idx = {
            re.sub(r'[^a-zA-Z0-9_]', '_', idx["name"]).lower(): idx
            for idx in tinfo.get("indexes", [])
        }
        tgt_idx = {k.lower(): v for k, v in tgt["indexes"].items()}
        for iname_lower, idx in src_idx.items():
            if iname_lower not in tgt_idx:
                changes["new_indexes"].append(idx)

        # Neue FKs
        src_fks = {
            re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"]).lower(): fk
            for fk in tinfo.get("fk", [])
        }
        tgt_fks = {k.lower(): v for k, v in tgt["fks"].items()}
        for fkname_lower, fk in src_fks.items():
            if fkname_lower not in tgt_fks:
                changes["new_fks"].append(fk)

        if any(changes.values()):
            diff["altered_tables"][tbl_name] = changes

    return diff


# ════════════════════════════════════════════════════════════════════════════
#  DDL aus Diff generieren
# ════════════════════════════════════════════════════════════════════════════

def generate_diff_ddl(diff: dict, source_schema: dict, target_db: str) -> Tuple[str, List[str]]:
    """Erzeugt inkrementelles DDL aus einem diff_schemas()-Ergebnis.

    Returns
    -------
    (ddl_string, warnings_list)
    """
    from transform import convert_default, generate_mysql_ddl

    lines: List[str] = [
        "-- Inkrementelles Schema-Update (Schema-Diff)",
        f"-- Zieldatenbank: {target_db}",
        "",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]
    warnings = list(diff["warnings"])

    # ── Neue Tabellen ─────────────────────────────────────────────────────
    if diff["new_tables"]:
        lines.append("-- Neue Tabellen")
    for tinfo in diff["new_tables"]:
        tname   = tinfo["name"]
        col_defs = []
        for c in tinfo["columns"]:
            mysql_type = convert_type(c["type"], c["max_len"], c["precision"], c["scale"])
            null_str   = "" if c["nullable"] else " NOT NULL"
            auto_str   = " AUTO_INCREMENT" if c["identity"] else ""
            default    = convert_default(c["default"]) if not c["identity"] else None
            def_str    = f" DEFAULT {default}" if default else ""
            col_defs.append(
                f"  {mssql_name(c['name'])} {mysql_type}{null_str}{auto_str}{def_str}"
            )
        if tinfo.get("pk"):
            pk_cols = ", ".join(mssql_name(p) for p in tinfo["pk"])
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        lines.append(f"CREATE TABLE IF NOT EXISTS {mssql_name(tname)} (")
        lines.append(",\n".join(col_defs))
        lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        lines.append("")

    # ── Bestehende Tabellen ändern ────────────────────────────────────────
    for tbl_name, changes in diff["altered_tables"].items():

        # Neue Spalten
        for col in changes["new_columns"]:
            mysql_type = convert_type(col["type"], col["max_len"], col["precision"], col["scale"])
            null_str   = "" if col["nullable"] else " NOT NULL"
            auto_str   = " AUTO_INCREMENT" if col["identity"] else ""
            default    = convert_default(col["default"]) if not col["identity"] else None
            def_str    = f" DEFAULT {default}" if default else ""
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"ADD COLUMN {mssql_name(col['name'])} "
                f"{mysql_type}{null_str}{auto_str}{def_str};"
            )

        # Geänderte Spaltentypen
        for col_name, old_type, new_type in changes["modified_columns"]:
            lines.append(
                f"-- ⚠ Typ-Änderung: {tbl_name}.{col_name} {old_type} → {new_type}"
            )
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"MODIFY COLUMN {mssql_name(col_name)} {new_type};"
            )

        # Neue Indexes
        for idx in changes["new_indexes"]:
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', idx["name"])
            unique_kw = "UNIQUE " if idx["unique"] else ""
            col_list  = ", ".join(
                f"{mssql_name(c['name'])} {'DESC' if c['desc'] else 'ASC'}"
                for c in idx["columns"]
            )
            lines.append(
                f"CREATE {unique_kw}INDEX `{safe_name}` "
                f"ON {mssql_name(tbl_name)} ({col_list});"
            )

        # Neue Foreign Keys
        for fk in changes["new_fks"]:
            safe_fk = re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"])
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({mssql_name(fk['from_col'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({mssql_name(fk['to_col'])});"
            )

    lines.append("")
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")

    # Warnung für entfernte Elemente am Ende
    if diff["removed_tables"] or diff["removed_columns"]:
        lines.append("")
        lines.append("-- ⚠ Folgende Elemente existieren in MySQL aber nicht mehr in der MDF:")
        for t in diff["removed_tables"]:
            lines.append(f"--   Tabelle: {t}")
        for t, cols in diff["removed_columns"].items():
            for c in cols:
                cname = c if isinstance(c, str) else c.get("name", str(c))
                lines.append(f"--   Spalte:  {t}.{cname}")
        lines.append("-- Diese wurden NICHT gelöscht. Manuelle Prüfung empfohlen.")

    return "\n".join(lines), warnings


# ════════════════════════════════════════════════════════════════════════════
#  Diff-Zusammenfassung als Text (für Log / Dry-Run-Preview)
# ════════════════════════════════════════════════════════════════════════════

def format_diff_summary(diff: dict) -> str:
    """Gibt eine lesbare Zusammenfassung des Diffs zurück."""
    lines = []

    if diff["new_tables"]:
        lines.append(f"  Neue Tabellen ({len(diff['new_tables'])}):")
        for t in diff["new_tables"]:
            lines.append(f"    + {t['name']}")

    if diff["altered_tables"]:
        lines.append(f"  Geänderte Tabellen ({len(diff['altered_tables'])}):")
        for tname, changes in diff["altered_tables"].items():
            if changes["new_columns"]:
                for c in changes["new_columns"]:
                    lines.append(f"    + {tname}.{c['name']} (neue Spalte)")
            if changes["modified_columns"]:
                for cname, old, new in changes["modified_columns"]:
                    lines.append(f"    ~ {tname}.{cname}: {old} → {new}  ⚠")
            if changes["new_indexes"]:
                for idx in changes["new_indexes"]:
                    lines.append(f"    + INDEX {idx['name']} ON {tname}")
            if changes["new_fks"]:
                for fk in changes["new_fks"]:
                    lines.append(f"    + FK {fk['name']} ON {tname}")

    if diff["removed_tables"]:
        lines.append(f"  Entfernte Tabellen ({len(diff['removed_tables'])}) – nur Warnung:")
        for t in diff["removed_tables"]:
            lines.append(f"    - {t}  (bleibt in MySQL)")

    if diff["removed_columns"]:
        lines.append("  Entfernte Spalten – nur Warnung:")
        for tname, cols in diff["removed_columns"].items():
            for c in cols:
                cname = c if isinstance(c, str) else c.get("name", str(c))
                lines.append(f"    - {tname}.{cname}  (bleibt in MySQL)")

    if not lines:
        lines.append("  Kein Unterschied gefunden – Schema ist aktuell.")

    return "\n".join(lines)

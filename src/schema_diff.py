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

from transform import convert_type, mssql_name, render_index_ddl, fk_actions_sql


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

    for tname, cname, ctype, nullable, default, extra, pos in cur.fetchall():
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
            "pos":         pos,
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

    # ── Foreign Keys (inkl. ON DELETE/UPDATE-Regeln) ──────────────────────
    cur.execute("""
        SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME,
               kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME,
               rc.DELETE_RULE, rc.UPDATE_RULE
        FROM   INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN   INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
               ON  tc.CONSTRAINT_NAME  = kcu.CONSTRAINT_NAME
               AND tc.TABLE_SCHEMA     = kcu.TABLE_SCHEMA
               AND tc.TABLE_NAME       = kcu.TABLE_NAME
        LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
               ON  rc.CONSTRAINT_NAME   = kcu.CONSTRAINT_NAME
               AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
        WHERE  kcu.TABLE_SCHEMA = %s
          AND  tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    """, (db_name,))
    for tname, fkname, col, ref_table, ref_col, del_rule, upd_rule in cur.fetchall():
        if tname in schema["tables"]:
            schema["tables"][tname]["fks"][fkname] = {
                "from_col":  col,
                "to_table":  ref_table,
                "to_col":    ref_col,
                "on_delete": del_rule or "RESTRICT",
                "on_update": upd_rule or "RESTRICT",
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
    t = re.sub(
        r'\bTINYINT\((\d+)\)',
        lambda m: 'TINYINT(1)' if m.group(1) == '1' else 'TINYINT',
        t,
    )
    # fsp 0 ist implizit: MySQL meldet DATETIME(0)/TIME(0) als datetime/time
    t = re.sub(r'\b(DATETIME|TIME|TIMESTAMP)\(0\)', r'\1', t)
    return t


def _source_mysql_type(col: dict) -> str:
    """Gibt den MySQL-Typ zurück den das MDF-Schema für diese Spalte vorsieht."""
    return _normalize_mysql_type(
        convert_type(col["type"], col.get("max_len"), col.get("precision"), col.get("scale"))
    )


def _normalize_fk_action(action: Optional[str]) -> str:
    """Normalisiert FK-Referenzaktionen für den Vergleich MSSQL ↔ MySQL.

    SQL Server meldet 'NO_ACTION', MySQL 'RESTRICT' bzw. 'NO ACTION' –
    alle drei verhalten sich in MySQL/InnoDB identisch.
    """
    a = (action or "NO_ACTION").upper().replace("_", " ")
    if a in ("NO ACTION", "RESTRICT"):
        return "RESTRICT"
    return a


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
            "modified_fks":     [],
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
                {**tgt["columns"][c], "name": c}
                for c in tgt["columns"] if c.lower() in removed
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

        # Neue + geänderte FKs (ON DELETE/UPDATE-Regel weicht ab)
        src_fks = {
            re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"]).lower(): fk
            for fk in tinfo.get("fk", [])
        }
        tgt_fks = {k.lower(): v for k, v in tgt["fks"].items()}
        for fkname_lower, fk in src_fks.items():
            if fkname_lower not in tgt_fks:
                changes["new_fks"].append(fk)
                continue
            tgt_fk  = tgt_fks[fkname_lower]
            src_del = _normalize_fk_action(fk.get("on_delete"))
            src_upd = _normalize_fk_action(fk.get("on_update"))
            tgt_del = _normalize_fk_action(tgt_fk.get("on_delete"))
            tgt_upd = _normalize_fk_action(tgt_fk.get("on_update"))
            if src_del != tgt_del or src_upd != tgt_upd:
                changes["modified_fks"].append(fk)
                diff["warnings"].append(
                    f"{tbl_name}.{fk['name']}: FK-Regel weicht ab "
                    f"(MySQL: ON DELETE {tgt_del}/ON UPDATE {tgt_upd} → "
                    f"Soll: ON DELETE {src_del}/ON UPDATE {src_upd}) – wird korrigiert."
                )

        if any(changes.values()):
            diff["altered_tables"][tbl_name] = changes

    return diff


# ════════════════════════════════════════════════════════════════════════════
#  Umbenennungs-Kandidaten erkennen (entfernte + neue Spalte = Rename?)
# ════════════════════════════════════════════════════════════════════════════

def _ordered_column_names(names_with_pos: List[Tuple[str, Any]]) -> List[str]:
    return [name for name, _ in sorted(names_with_pos, key=lambda kv: kv[1] or 0)]


def _neighbors(ordered_names: List[str], name: str) -> Tuple[Optional[str], Optional[str]]:
    """Liefert (Vorgänger, Nachfolger) von `name` in `ordered_names` (lowercase)."""
    lname = name.lower()
    try:
        i = next(idx for idx, n in enumerate(ordered_names) if n.lower() == lname)
    except StopIteration:
        return (None, None)
    prev_ = ordered_names[i - 1].lower() if i > 0 else None
    next_ = ordered_names[i + 1].lower() if i + 1 < len(ordered_names) else None
    return (prev_, next_)


def detect_rename_candidates(
    diff: dict, source_schema: dict, target_mysql: dict
) -> Dict[str, List[Tuple[str, str, str]]]:
    """Erkennt pro Tabelle Spalten, die vermutlich nur umbenannt wurden.

    Eine entfernte Spalte (existiert in MySQL, nicht mehr in der MDF) und
    eine neue Spalte (existiert in der MDF, nicht in MySQL) gelten als
    Umbenennungs-Kandidat, wenn ihr MySQL-Zieltyp identisch ist.

    Gibt es pro Tabelle mehr als ein Kandidatenpaar mit passendem Typ, ist
    die Zuordnung unsicher (z.B. zwei DOUBLE-Spalten gleichzeitig entfernt
    und hinzugefügt). In diesem Fall wird zusätzlich verlangt, dass die
    Vorgänger- und Nachfolger-Spalte (Position -1/+1) beider Seiten
    übereinstimmen – andernfalls wird das Paar verworfen, da der Bezug zu
    unsicher ist.

    Returns
    -------
    {table_name: [(alter_name, neuer_name, mysql_typ), ...]}
    """
    src_tables_by_name = {t["name"]: t for t in source_schema.get("tables", {}).values()}
    candidates: Dict[str, List[Tuple[str, str, str]]] = {}

    for tbl_name, changes in diff["altered_tables"].items():
        removed_cols = diff["removed_columns"].get(tbl_name, [])
        new_cols     = changes["new_columns"]
        if not removed_cols or not new_cols:
            continue

        src_tinfo = src_tables_by_name.get(tbl_name)
        tgt_tinfo = (
            target_mysql["tables"].get(tbl_name)
            or target_mysql["tables"].get(tbl_name.lower())
        )
        if not src_tinfo or not tgt_tinfo:
            continue

        # Kandidatenpaare nach übereinstimmendem Typ gruppieren - Mehrdeutigkeit
        # (mehrere Kandidaten desselben Typs) wird pro Typ einzeln beurteilt,
        # damit ein eindeutiges INT-Paar nicht an einem mehrdeutigen
        # DOUBLE-Paar in derselben Tabelle scheitert.
        pairs_by_type: Dict[str, List[Tuple[str, str]]] = {}
        for old in removed_cols:
            old_type = _normalize_mysql_type(old["type"])
            for new in new_cols:
                new_type = _source_mysql_type(new)
                if old_type == new_type:
                    pairs_by_type.setdefault(new_type, []).append((old["name"], new["name"]))

        if not pairs_by_type:
            continue

        src_names = _ordered_column_names(
            [(c["name"], c.get("pos")) for c in src_tinfo["columns"]]
        )
        tgt_names = _ordered_column_names(
            [(n, c.get("pos")) for n, c in tgt_tinfo["columns"].items()]
        )

        for mtype, type_pairs in pairs_by_type.items():
            if len(type_pairs) == 1:
                old_name, new_name = type_pairs[0]
                candidates.setdefault(tbl_name, []).append((old_name, new_name, mtype))
                continue
            # Mehrdeutig: nur Paare behalten, deren Nachbar-Spalten übereinstimmen
            for old_name, new_name in type_pairs:
                if _neighbors(tgt_names, old_name) == _neighbors(src_names, new_name):
                    candidates.setdefault(tbl_name, []).append((old_name, new_name, mtype))

    return candidates


# ════════════════════════════════════════════════════════════════════════════
#  DDL aus Diff generieren
# ════════════════════════════════════════════════════════════════════════════

def generate_diff_ddl(
    diff: dict,
    source_schema: dict,
    target_db: str,
    rename_pairs: Optional[Dict[str, List[Tuple[str, str]]]] = None,
) -> Tuple[str, List[str]]:
    """Erzeugt inkrementelles DDL aus einem diff_schemas()-Ergebnis.

    `rename_pairs` (von detect_rename_candidates(), nach User-Bestätigung):
    {table_name: [(alter_name, neuer_name), ...]}. Für jedes Paar wird nach
    dem ADD COLUMN ein `UPDATE ... SET neu = alt;` ergänzt, damit die
    bestehenden Werte in die umbenannte Spalte übernommen werden.

    Returns
    -------
    (ddl_string, warnings_list)
    """
    from transform import convert_default, generate_mysql_ddl

    rename_pairs = rename_pairs or {}
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
            default    = convert_default(c["default"], mysql_type) if not c["identity"] else None
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
            default    = convert_default(col["default"], mysql_type) if not col["identity"] else None
            def_str    = f" DEFAULT {default}" if default else ""
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"ADD COLUMN {mssql_name(col['name'])} "
                f"{mysql_type}{null_str}{auto_str}{def_str};"
            )

        # Umbenennungen (User-bestätigt): Werte übernehmen, alte Spalte löschen
        for old_name, new_name in rename_pairs.get(tbl_name, []):
            lines.append(
                f"-- Umbenennung übernommen: {tbl_name}.{old_name} → {new_name}"
            )
            lines.append(
                f"UPDATE {mssql_name(tbl_name)} "
                f"SET {mssql_name(new_name)} = {mssql_name(old_name)};"
            )
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"DROP COLUMN {mssql_name(old_name)};"
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
            lines.extend(render_index_ddl(tbl_name, idx))

        # Neue Foreign Keys
        for fk in changes["new_fks"]:
            safe_fk = re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"])
            actions, fk_warns = fk_actions_sql(fk)
            for w in fk_warns:
                lines.append(f"-- ⚠ {w}")
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({mssql_name(fk['from_col'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({mssql_name(fk['to_col'])})"
                f"{actions};"
            )

        # Geänderte FK-Regeln: DROP + ADD mit korrekter ON DELETE/UPDATE-Klausel
        for fk in changes.get("modified_fks", []):
            safe_fk = re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"])
            actions, fk_warns = fk_actions_sql(fk)
            for w in fk_warns:
                lines.append(f"-- ⚠ {w}")
            lines.append(
                f"-- FK-Regel korrigieren: {fk['name']}"
                f" (ON DELETE {_normalize_fk_action(fk.get('on_delete'))})"
            )
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} DROP FOREIGN KEY `{safe_fk}`;"
            )
            lines.append(
                f"ALTER TABLE {mssql_name(tbl_name)} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({mssql_name(fk['from_col'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({mssql_name(fk['to_col'])})"
                f"{actions};"
            )

    lines.append("")
    lines.append("SET FOREIGN_KEY_CHECKS = 1;")

    # Warnung für entfernte Elemente am Ende (umbenannte Spalten ausgenommen,
    # die wurden oben bereits per DROP COLUMN entfernt)
    renamed_old_cols = {
        (tbl.lower(), old_name.lower())
        for tbl, pairs in rename_pairs.items()
        for old_name, _ in pairs
    }
    remaining_removed_columns = {
        t: [
            c for c in cols
            if (t.lower(), (c if isinstance(c, str) else c.get("name", str(c))).lower())
            not in renamed_old_cols
        ]
        for t, cols in diff["removed_columns"].items()
    }
    remaining_removed_columns = {t: cols for t, cols in remaining_removed_columns.items() if cols}

    if diff["removed_tables"] or remaining_removed_columns:
        lines.append("")
        lines.append("-- ⚠ Folgende Elemente existieren in MySQL aber nicht mehr in der MDF:")
        for t in diff["removed_tables"]:
            lines.append(f"--   Tabelle: {t}")
        for t, cols in remaining_removed_columns.items():
            for c in cols:
                cname = c if isinstance(c, str) else c.get("name", str(c))
                lines.append(f"--   Spalte:  {t}.{cname}")
        lines.append("-- Diese wurden NICHT gelöscht. Manuelle Prüfung empfohlen.")

    return "\n".join(lines), warnings


# ════════════════════════════════════════════════════════════════════════════
#  Diff-Zusammenfassung als Text (für Log / Dry-Run-Preview)
# ════════════════════════════════════════════════════════════════════════════

def get_tables_to_refresh(diff: dict) -> set:
    """Gibt die Tabellennamen (Kleinbuchstaben) zurück, deren Daten neu geladen
    werden sollten, basierend auf dem Schema-Diff.

    Regeln:
      - Neue Tabellen:          leer → Daten laden (kein TRUNCATE nötig)
      - Typ-Änderung einer Spalte:    Daten könnten inkompatibel sein → neu laden
      - Neue Spalte:            Daten aus MDF können neue Spalte befüllen → neu laden
      - Nur neue Indexes / FKs: Daten unverändert gültig → KEIN Reload
      - Entfernte Tabellen/Spalten:   werden nicht berührt
    """
    tables: set = set()

    for tinfo in diff.get("new_tables", []):
        tables.add(tinfo["name"].lower())

    for tname, changes in diff.get("altered_tables", {}).items():
        if changes.get("new_columns") or changes.get("modified_columns"):
            tables.add(tname.lower())

    return tables


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
            if changes.get("modified_fks"):
                for fk in changes["modified_fks"]:
                    lines.append(
                        f"    ~ FK {fk['name']} ON {tname}: "
                        f"Regel → ON DELETE {_normalize_fk_action(fk.get('on_delete'))}  ⚠"
                    )

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

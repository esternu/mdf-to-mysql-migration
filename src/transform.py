"""
SQL Server → MySQL Transformation.
Enthält:
  - Typ-Konvertierung  (TYPE_MAP, convert_type, mssql_name, convert_default)
  - View-Konvertierung (convert_view_sql, _convert_apply_to_join, …)
  - DDL-Generierung    (generate_mysql_ddl, _topo_sort_views)
"""
import re
from typing import Dict, List, Optional


# ════════════════════════════════════════════════════════════════════════════
#  Typ-Konvertierung
# ════════════════════════════════════════════════════════════════════════════
TYPE_MAP: Dict[str, str] = {
    "nvarchar":        "VARCHAR",
    "nchar":           "CHAR",
    "ntext":           "LONGTEXT",
    "varchar":         "VARCHAR",
    "char":            "CHAR",
    "text":            "LONGTEXT",
    "int":             "INT",
    "bigint":          "BIGINT",
    "smallint":        "SMALLINT",
    "tinyint":         "TINYINT",
    "bit":             "TINYINT(1)",
    "decimal":         "DECIMAL",
    "numeric":         "DECIMAL",
    "float":           "DOUBLE",
    "real":            "FLOAT",
    "money":           "DECIMAL(19,4)",
    "smallmoney":      "DECIMAL(10,4)",
    "datetime":        "DATETIME",
    "datetime2":       "DATETIME(6)",
    "smalldatetime":   "DATETIME",
    "date":            "DATE",
    "time":            "TIME",
    "datetimeoffset":  "DATETIME(6)",
    "timestamp":       "TIMESTAMP",
    "uniqueidentifier": "CHAR(36)",
    "varbinary":       "LONGBLOB",
    "binary":          "BINARY",
    "image":           "LONGBLOB",
    "xml":             "LONGTEXT",
    "geography":       "LONGTEXT",
    "geometry":        "LONGTEXT",
    "hierarchyid":     "VARCHAR(255)",
    "sql_variant":     "LONGTEXT",
}


def convert_type(sql_type: str, max_len, precision, scale) -> str:
    """Konvertiert einen SQL-Server-Datentyp in den entsprechenden MySQL-Typ."""
    base  = sql_type.lower().strip()
    mysql = TYPE_MAP.get(base, sql_type.upper())

    if mysql in ("VARCHAR", "CHAR") and max_len is not None:
        ml = int(max_len)
        if ml == -1:
            return "LONGTEXT"   # NVARCHAR(MAX) / VARCHAR(MAX)
        # utf8mb4: max. 4 Bytes/Zeichen → VARCHAR-Limit = 16 383
        if mysql == "CHAR":
            return f"CHAR({min(ml, 255)})"
        if ml > 16383:
            return "TEXT" if ml <= 65535 else "MEDIUMTEXT"
        return f"VARCHAR({ml})"

    if mysql == "DECIMAL" and precision:
        sc = scale or 0
        return f"DECIMAL({precision},{sc})"
    return mysql


def mssql_name(name: str) -> str:
    """SQL-Server-Bezeichner (eckige Klammern) → MySQL-Backtick."""
    return f"`{name.strip('[]')}`"


def convert_default(default_val: Optional[str]) -> Optional[str]:
    """Konvertiert einen SQL-Server-DEFAULT-Ausdruck in MySQL-Syntax.

    SQL Server speichert Defaults als '(expr)' oder '((expr))'.
    strip("()") entfernt alle führenden/abschliessenden Klammern zeichenweise,
    sodass '(getdate())' → 'getdate' wird (nicht 'getdate()').
    """
    if default_val is None:
        return None
    d     = default_val.strip().strip("()")
    lower = d.lower()
    # Nach strip("()") sind Klammern bereits entfernt
    if lower in ("getdate", "getutcdate"):
        return "CURRENT_TIMESTAMP"
    if lower == "newid":
        return None   # UUID() als DEFAULT nur ab MySQL 8.x
    if lower == "1":
        return "'1'"
    if lower == "0":
        return "'0'"
    d = d.strip("'\"")
    return f"'{d}'" if d else None


# ════════════════════════════════════════════════════════════════════════════
#  Hilfsfunktionen für View-Konvertierung
# ════════════════════════════════════════════════════════════════════════════
def _paren_close(sql: str, open_pos: int) -> int:
    """Gibt den Index der schliessenden ')' zurück, die zu '(' an open_pos gehört."""
    depth = 1
    i = open_pos + 1
    while i < len(sql) and depth > 0:
        if sql[i] == '(':
            depth += 1
        elif sql[i] == ')':
            depth -= 1
        i += 1
    return i - 1


def _convert_apply_to_join(sql: str) -> str:
    """Konvertiert OUTER/CROSS APPLY zu LEFT JOIN / JOIN mit gruppierter Subquery.

    OUTER APPLY → LEFT JOIN (subquery + GROUP BY) ON join_condition
    CROSS APPLY → JOIN      (subquery + GROUP BY) ON join_condition

    Vermeidet LATERAL, das auf manchen MariaDB-Builds nicht verfügbar ist.
    """
    apply_re = re.compile(r'\b(OUTER|CROSS)\s+APPLY\s*\(', re.IGNORECASE)
    result: List[str] = []
    pos = 0

    for m in apply_re.finditer(sql):
        join_kw = 'LEFT JOIN' if m.group(1).upper() == 'OUTER' else 'JOIN'
        result.append(sql[pos:m.start()])

        open_pos  = m.end() - 1
        close_pos = _paren_close(sql, open_pos)
        body      = sql[m.end():close_pos]

        after   = sql[close_pos + 1:]
        alias_m = re.match(r'\s+(?:AS\s+)?(`?\w+`?)', after, re.IGNORECASE)
        alias     = alias_m.group(1) if alias_m else 'subq'
        alias_end = close_pos + 1 + (len(alias_m.group(0)) if alias_m else 0)

        from_m  = re.search(r'\bFROM\b',  body, re.IGNORECASE)
        where_m = re.search(r'\bWHERE\b', body, re.IGNORECASE)

        if from_m and where_m:
            select_part = body[:from_m.start()].strip()
            from_part   = body[from_m.end():where_m.start()].strip()
            where_part  = body[where_m.end():].strip()

            inner_alias_m = re.search(r'(?:AS\s+)?(`?\w+`?)\s*$', from_part, re.IGNORECASE)
            inner_alias   = inner_alias_m.group(1).strip('`') if inner_alias_m else ''

            conditions = re.split(r'\bAND\b', where_part, flags=re.IGNORECASE)
            correlated: List[tuple] = []
            remaining:  List[str]   = []

            for cond in conditions:
                c  = cond.strip()
                eq = re.match(
                    r'(`?\w+`?)\.(`?\w+`?)\s*=\s*(`?\w+`?)\.(`?\w+`?)',
                    c, re.IGNORECASE
                )
                if eq:
                    l_tbl, l_col, r_tbl, r_col = [g.strip('`') for g in eq.groups()]
                    if l_tbl.lower() == inner_alias.lower():
                        correlated.append((l_col, f'{r_tbl}.{r_col}'))
                        continue
                    elif r_tbl.lower() == inner_alias.lower():
                        correlated.append((r_col, f'{l_tbl}.{l_col}'))
                        continue
                remaining.append(c)

            if correlated:
                extra_sel  = ', '.join(f'{inner_alias}.{c[0]}' for c in correlated)
                new_select = select_part + ', ' + extra_sel
                group_by   = ', '.join(f'{inner_alias}.{c[0]}' for c in correlated)
                on_cond    = ' AND '.join(f'{alias}.{c[0]} = {c[1]}' for c in correlated)

                new_body = f'{new_select}\n    FROM {from_part}'
                if remaining:
                    new_body += '\n    WHERE ' + ' AND '.join(remaining)
                new_body += f'\n    GROUP BY {group_by}'

                result.append(f'{join_kw} (\n    {new_body}\n) AS {alias} ON {on_cond}')
                pos = alias_end
                continue

        # Fallback: Body unverändert übernehmen
        result.append(f'{join_kw} ({body}) AS {alias}')
        pos = alias_end

    result.append(sql[pos:])
    return ''.join(result)


def convert_view_sql(tsql: str) -> tuple:
    """T-SQL → MySQL Konvertierung für View-Definitionen.

    Gibt (sql, warnings) zurück.
    warnings ist eine Liste von Strings für Konstrukte die manuell
    nachbearbeitet werden müssen.
    """
    sql = tsql.strip()
    warnings: List[str] = []

    # ── CREATE VIEW Header entfernen ──────────────────────────────────────
    # sys.sql_modules liefert den vollständigen T-SQL-Text inkl.
    # "CREATE VIEW [dbo].[Name] AS" – das wird von uns neu generiert.
    # KEIN DOTALL: verhindert, dass der Regex über Zeilenenden greift.
    sql = re.sub(
        r'^\s*CREATE\s+VIEW\s+'
        r'(?:\[[\w\s]+\]|\w+)'
        r'(?:\.(?:\[[\w\s]+\]|\w+))?'
        r'\s*(?:\([^)]*\))?\s*\bAS\b\s*',
        '',
        sql,
        count=1,
        flags=re.IGNORECASE,
    ).strip()

    # ── Schema-Präfixe entfernen: [dbo]. und dbo. ─────────────────────────
    sql = re.sub(r'\[dbo\]\.', '', sql)
    sql = re.sub(r'\bdbo\.', '', sql)

    # Bezeichner: [Name] → `Name`
    sql = re.sub(r'\[([^\]]+)\]', r'`\1`', sql)

    # ── SQL-Server-Datentypen in View-Körpern ersetzen ────────────────────
    _VIEW_TYPE_MAP = [
        (r'\bmoney\b',             'DECIMAL(19,4)'),
        (r'\bsmallmoney\b',        'DECIMAL(10,4)'),
        (r'\bnvarchar\b',          'CHAR'),
        (r'\bnchar\b',             'CHAR'),
        (r'\bntext\b',             'TEXT'),
        (r'\bdatetime2\b',         'DATETIME'),
        (r'\bsmalldatetime\b',     'DATETIME'),
        (r'\bdatetimeoffset\b',    'DATETIME'),
        (r'\buniqueidentifier\b',  'CHAR(36)'),
        (r'\bbit\b',               'TINYINT(1)'),
        (r'\bimage\b',             'LONGBLOB'),
        (r'\bsql_variant\b',       'TEXT'),
    ]
    for pattern, replacement in _VIEW_TYPE_MAP:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    # ── T-SQL Funktionen → MySQL ──────────────────────────────────────────
    sql = re.sub(r'\bGETDATE\s*\(\s*\)',     'NOW()',           sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGETUTCDATE\s*\(\s*\)', 'UTC_TIMESTAMP()', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bISNULL\s*\(',           'IFNULL(',         sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bIIF\s*\(',              'IF(',             sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bLEN\s*\(',              'LENGTH(',         sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCHARINDEX\s*\(([^,]+),([^)]+)\)',
                 r'LOCATE(\1,\2)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSUBSTRING\s*\(',       'SUBSTRING(',      sql, flags=re.IGNORECASE)

    # WITH (NOLOCK) entfernen
    sql = re.sub(r'\bWITH\s*\(\s*NOLOCK\s*\)', '', sql, flags=re.IGNORECASE)

    # TOP n entfernen
    sql = re.sub(r'\bTOP\s+\d+\b', '', sql, flags=re.IGNORECASE)

    # ── STRING_AGG → GROUP_CONCAT ─────────────────────────────────────────
    def _string_agg_repl(m: re.Match) -> str:
        expr      = m.group(1).strip()
        separator = m.group(2).strip().strip("'\"")
        order_col = m.group(3).strip() if m.group(3) else None
        if order_col:
            return f"GROUP_CONCAT({expr} ORDER BY {order_col} SEPARATOR '{separator}')"
        return f"GROUP_CONCAT({expr} SEPARATOR '{separator}')"

    # Mit WITHIN GROUP
    sql = re.sub(
        r'\bSTRING_AGG\s*\(\s*(.+?)\s*,\s*([\'"][^\'"]*[\'"])\s*\)'
        r'\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\s*\)',
        _string_agg_repl,
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Ohne WITHIN GROUP
    sql = re.sub(
        r'\bSTRING_AGG\s*\(\s*(.+?)\s*,\s*([\'"][^\'"]*[\'"])\s*\)',
        lambda m: (
            f"GROUP_CONCAT({m.group(1).strip()} SEPARATOR "
            f"'{m.group(2).strip().strip(chr(39)+chr(34))}')"
        ),
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    # ── OUTER/CROSS APPLY → LEFT JOIN / JOIN (grouped subquery) ──────────
    sql = _convert_apply_to_join(sql)

    # Doppelte Leerzeilen bereinigen
    sql = re.sub(r'\n{3,}', '\n\n', sql)

    return sql.strip(), warnings


# ════════════════════════════════════════════════════════════════════════════
#  Topologischer Sort für Views
# ════════════════════════════════════════════════════════════════════════════
def _topo_sort_views(views: dict) -> list:
    """Sortiert Views topologisch: zuerst Views ohne Abhängigkeiten,
    dann Views die andere Views referenzieren (Kahn's Algorithmus)."""
    view_names = {v["name"].lower() for v in views.values()}

    deps: Dict[str, set] = {}
    for vinfo in views.values():
        body  = vinfo["definition"].lower()
        found = set()
        for other in view_names:
            if other == vinfo["name"].lower():
                continue
            if re.search(r'\b' + re.escape(other) + r'\b', body):
                found.add(other)
        deps[vinfo["name"].lower()] = found

    sorted_list: list  = []
    remaining          = {v["name"].lower(): v for v in views.values()}
    iterations         = 0

    while remaining and iterations < len(views) + 1:
        iterations += 1
        ready = [
            name for name in remaining
            if not (deps[name] & set(remaining.keys()) - {name})
        ]
        if not ready:
            ready = list(remaining.keys())   # Zyklus: Rest anhängen
        for name in sorted(ready):
            sorted_list.append(remaining.pop(name))

    return sorted_list


# ════════════════════════════════════════════════════════════════════════════
#  DDL-Generierung
# ════════════════════════════════════════════════════════════════════════════
def generate_mysql_ddl(schema: dict, target_db: str) -> str:
    """Erzeugt vollständiges MySQL-DDL aus dem gelesenen Schema-Dict."""
    lines = [
        "-- Generiert von MDF-to-MySQL Migration Tool",
        f"-- Quelldatenbank aus .mdf → Ziel: {target_db}",
        "",
        f"CREATE DATABASE IF NOT EXISTS `{target_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
        f"USE `{target_db}`;",
        "SET FOREIGN_KEY_CHECKS = 0;",
        "",
    ]

    # ── Tabellen ──────────────────────────────────────────────────────────
    for tinfo in schema["tables"].values():
        tname    = tinfo["name"]
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
        if tinfo["pk"]:
            pk_cols = ", ".join(mssql_name(p) for p in tinfo["pk"])
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        lines.append(f"DROP TABLE IF EXISTS {mssql_name(tname)};")
        lines.append(f"CREATE TABLE {mssql_name(tname)} (")
        lines.append(",\n".join(col_defs))
        lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        lines.append("")

    # ── Foreign Keys ──────────────────────────────────────────────────────
    for tinfo in schema["tables"].values():
        for fk in tinfo["fk"]:
            safe_fk = re.sub(r'[^a-zA-Z0-9_]', '_', fk["name"])
            lines.append(
                f"ALTER TABLE {mssql_name(tinfo['name'])} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({mssql_name(fk['from_col'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({mssql_name(fk['to_col'])});"
            )
    if any(tinfo["fk"] for tinfo in schema["tables"].values()):
        lines.append("")

    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    lines.append("")

    # ── Views (topologisch sortiert) ──────────────────────────────────────
    for vinfo in _topo_sort_views(schema["views"]):
        vname        = vinfo["name"]
        vdef, warns  = convert_view_sql(vinfo["definition"])
        lines.append(f"DROP VIEW IF EXISTS {mssql_name(vname)};")
        for w in warns:
            lines.append(f"-- ⚠ {w}")
        lines.append(f"CREATE VIEW {mssql_name(vname)} AS")
        lines.append(vdef + ";")
        lines.append("")

    return "\n".join(lines)

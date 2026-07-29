"""
SQL Server → MySQL Transformation.
Enthält:
  - Typ-Konvertierung  (TYPE_MAP, convert_type, mssql_name, convert_default)
  - View-Konvertierung (convert_view_sql, _convert_apply_to_join, …)
  - DDL-Generierung    (generate_mysql_ddl, _topo_sort_views)
"""
import re
from typing import Dict, List, Optional, Tuple

# Views, die nicht uebersetzt werden: ViewAuditChanges basiert auf MSSQL-
# spezifischem OPENJSON/FULL OUTER JOIN und wird durch die Audit-Trigger
# (audit_triggers.py, P4) ersetzt -> aus dem generierten DDL ausgeschlossen.
EXCLUDED_VIEWS = {"viewauditchanges"}


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
    "tinyint":         "TINYINT UNSIGNED",
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
    # SQL-Server-"timestamp" ist ein Synonym fuer rowversion (8-Byte-
    # Nebenlaeufigkeitszaehler), KEINE Uhrzeit. MySQL TIMESTAMP waere eine
    # echte Zeit (Bereich nur 1970-2038 UTC) - falsch. -> BINARY(8).
    "timestamp":       "BINARY(8)",
    "rowversion":      "BINARY(8)",
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

    # datetime2(s)/datetimeoffset(s)/time(s): Praezision (fsp) uebernehmen.
    # SQL Server erlaubt 0-7, MySQL 0-6 → Cap bei 6. Ohne Angabe (None)
    # bleibt der bisherige sichere Default 6. fsp 0 → Typ ohne Klammer
    # (MySQL meldet DATETIME(0) als "datetime" zurueck).
    if base in ("datetime2", "datetimeoffset", "time"):
        fsp    = min(int(scale), 6) if scale is not None else 6
        target = "TIME" if base == "time" else "DATETIME"
        return f"{target}({fsp})" if fsp > 0 else target

    # binary(n)/varbinary(n): Laenge uebernehmen, sonst legt MySQL BINARY(1)
    # an und trunkiert Daten. varbinary(max) (= max_len -1) bleibt LONGBLOB.
    if base in ("binary", "varbinary") and max_len is not None:
        ml = int(max_len)
        if ml == -1:
            return "LONGBLOB"
        if base == "binary":
            return f"BINARY({min(ml, 255)})"
        return f"VARBINARY({ml})" if ml <= 65532 else "LONGBLOB"

    if mysql == "DECIMAL" and precision:
        sc = scale or 0
        return f"DECIMAL({precision},{sc})"
    return mysql


def mssql_name(name: str) -> str:
    """SQL-Server-Bezeichner (eckige Klammern) → MySQL-Backtick."""
    return f"`{name.strip('[]')}`"


def _strip_balanced_parens(s: str) -> str:
    """Entfernt aeussere Klammerpaare nur, wenn sie balanciert das GANZE
    Argument umschliessen. '(N''Wert (intern)'')' verliert so nie die
    innere schliessende Klammer (zeichenweises strip('()') tat das)."""
    s = s.strip()
    while s.startswith("(") and s.endswith(")"):
        depth = 0
        balanced = True
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i < len(s) - 1:
                    balanced = False   # aeussere Klammer schliesst zu frueh
                    break
        if not balanced or depth != 0:
            break
        s = s[1:-1].strip()
    return s


def convert_default(default_val: Optional[str], mysql_type: Optional[str] = None) -> Optional[str]:
    """Konvertiert einen SQL-Server-DEFAULT-Ausdruck in MySQL-Syntax.

    SQL Server speichert Defaults als '(expr)' oder '((expr))' – die
    aeusseren Klammern werden balanciert entfernt (Klammern IM Wert
    bleiben erhalten). ``N'…'``-Unicode-Literale verlieren ihr Praefix.

    ``mysql_type`` (optional): Zieltyp der Spalte. Bei GETDATE()-Defaults
    auf DATETIME(n)-Spalten wird CURRENT_TIMESTAMP(n) erzeugt – striktes
    MySQL 8 verlangt uebereinstimmende fsp (MariaDB toleriert beides).
    """
    if default_val is None:
        return None
    d = _strip_balanced_parens(default_val)
    # Aktuelle-Zeit-Funktionen von SQL Server auf CURRENT_TIMESTAMP abbilden:
    # getdate/getutcdate (datetime), sysdatetime/sysutcdatetime (datetime2),
    # sysdatetimeoffset (datetimeoffset) und ANSI current_timestamp - jeweils
    # mit ODER ohne Klammern. Ohne diese Erkennung landet z.B. sysdatetime()
    # als String-Literal 'sysdatetime()' im DEFAULT -> MariaDB Error 1067.
    # newid() -> kein Default (UUID() als DEFAULT erst ab MySQL 8.x).
    func = re.fullmatch(
        r'(getdate|getutcdate|sysdatetime|sysutcdatetime|sysdatetimeoffset|'
        r'current_timestamp|newid)\s*(?:\(\s*\))?',
        d, flags=re.IGNORECASE)
    lower = func.group(1).lower() if func else d.lower()

    _NOW_FUNCS = {"getdate", "getutcdate", "sysdatetime", "sysutcdatetime",
                  "sysdatetimeoffset", "current_timestamp"}
    if lower in _NOW_FUNCS:
        # fsp der Zielspalte anhaengen (striktes MySQL 8 verlangt Gleichheit)
        m = re.fullmatch(r'DATETIME\((\d)\)', (mysql_type or "").strip(), flags=re.IGNORECASE)
        return f"CURRENT_TIMESTAMP({m.group(1)})" if m else "CURRENT_TIMESTAMP"
    if lower == "newid":
        return None   # UUID() als DEFAULT nur ab MySQL 8.x
    if lower == "1":
        return "'1'"
    if lower == "0":
        return "'0'"

    # N'…' → '…' (MySQL braucht das Unicode-Praefix nicht; im DEFAULT
    # wuerde der strip unten sonst ein kaputtes Literal erzeugen)
    m = re.fullmatch(r"N\s*'(.*)'", d, flags=re.IGNORECASE | re.DOTALL)
    if m:
        d = m.group(1)
    else:
        d = d.strip("'\"")
    d = d.replace("''", "'").replace("'", "''")   # Escaping normalisieren
    return f"'{d}'" if d else None


# ════════════════════════════════════════════════════════════════════════════
#  Hilfsfunktionen für View-Konvertierung
# ════════════════════════════════════════════════════════════════════════════

# Operanden die in einer String-Konkatenationskette vorkommen können:
#   - einfaches String-Literal:  'text'
#   - Backtick-Bezeichner:       `col name`
#   - CAST-Ausdruck:             CAST(x AS CHAR(n))  ← eine Ebene Nesting erlaubt
# Bewusst KEINE einfachen Wörter (verhindert, dass arithmetische Ausdrücke
# wie "a + b" fälschlich als String-Konkatenation erkannt werden).
_STR_OPERAND = (
    r"(?:"
    r"'[^']*'"                              r"|"  # 'string literal'
    r"`[^`]+`"                              r"|"  # `backtick identifier`
    r"CAST\s*\([^()]*(?:\([^()]*\)[^()]*)*\)"    # CAST(x AS CHAR(n)) – 1 Ebene Nesting
    r")"
)

# Kette: operand (Leerzeichen + Leerzeichen operand)+
_STR_CONCAT_CHAIN = re.compile(
    r"(" + _STR_OPERAND + r"(?:\s*\+\s*" + _STR_OPERAND + r")+)",
    re.IGNORECASE,
)


def _convert_string_concat(sql: str) -> str:
    """Ersetzt T-SQL-String-Konkatenation mit ``+`` durch MySQL ``CONCAT()``.

    T-SQL: ``col + ' (' + CAST(id AS CHAR(10)) + ')'``
    MySQL: ``CONCAT(col, ' (', CAST(id AS CHAR(10)), ')')``

    Konvertiert **nur** Ketten, in denen mindestens ein Operand ein
    einfaches String-Literal (``'...'``) ist.  Rein arithmetische
    Ausdrücke (z. B. ``surface + jig_surface``) bleiben unberührt,
    weil sie keine Backtick-Bezeichner oder CAST-Ausdrücke sind, die
    mit einem String-Literal gemischt werden.
    """
    def _replace(m: re.Match) -> str:
        chain = m.group(0)
        # Nur konvertieren wenn mindestens ein String-Literal vorhanden
        if not re.search(r"'[^']*'", chain):
            return chain
        parts = [p.strip() for p in re.split(r"\s*\+\s*", chain)]
        return "CONCAT(" + ", ".join(parts) + ")"

    return _STR_CONCAT_CHAIN.sub(_replace, sql)


# CAST-Zieltypen: T-SQL-Typname → gueltiger MySQL-CAST-Typ.
# (nvarchar/money etc. sind zu diesem Zeitpunkt bereits durch die
# _VIEW_TYPE_MAP-Ersetzungen in CHAR/DECIMAL(19,4) umgeschrieben.)
_CAST_TYPE_MAP = {
    "INT": "SIGNED", "BIGINT": "SIGNED", "SMALLINT": "SIGNED",
    "TINYINT": "UNSIGNED", "BIT": "UNSIGNED",
    "FLOAT": "DOUBLE", "REAL": "FLOAT", "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL", "NUMERIC": "DECIMAL",
    "VARCHAR": "CHAR", "NVARCHAR": "CHAR", "CHAR": "CHAR", "NCHAR": "CHAR",
    "DATETIME": "DATETIME", "DATETIME2": "DATETIME", "SMALLDATETIME": "DATETIME",
    "DATE": "DATE", "TIME": "TIME",
}

# CAST-Typen, die keine Laengenangabe erlauben/brauchen
_CAST_NO_LENGTH = {"SIGNED", "UNSIGNED", "DOUBLE", "FLOAT", "DATE", "DATETIME", "TIME"}


def _split_top_level_commas(s: str) -> List[str]:
    """Teilt einen Argument-String an Kommas der obersten Klammerebene.
    Kommas in Klammern und String-Literalen werden ignoriert."""
    parts: List[str] = []
    buf:   List[str] = []
    depth  = 0
    in_str = False
    for ch in s:
        if in_str:
            buf.append(ch)
            if ch == "'":
                in_str = False
            continue
        if ch == "'":
            in_str = True
            buf.append(ch)
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            parts.append(''.join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append(''.join(buf))
    return parts


def extract_view_columns(sql: str) -> List[str]:
    """Extrahiert die Ausgabe-Spaltennamen aus dem AEUSSERSTEN SELECT einer
    (bereits nach MySQL konvertierten) View-Definition.

    Wird vom Schema-Diff genutzt, um geaenderte/umbenannte View-Spalten zu
    erkennen (Spaltensignatur-Vergleich gegen die Live-DB). Der Parser ist
    klammertiefen- und string-literal-bewusst und kommt mit CTEs (WITH … AS
    (…) … SELECT) zurecht: der finale, oberste SELECT liefert die Spalten.

    Gibt [] zurueck, wenn die Spaltenliste nicht sicher bestimmt werden kann -
    der Aufrufer behandelt die View dann als 'geaendert' und erstellt sie neu
    (sicherer Fallback, da Views datenlos sind).
    """
    s = sql.strip().rstrip(";")
    # Zeilenkommentare entfernen (die WCEP-Views nutzen ----/-- OUTPUT-Trenner)
    s = re.sub(r'--[^\n]*', '', s)

    # Positionen aller SELECT/FROM auf Klammer-Ebene 0 sammeln
    depth  = 0
    in_str = False
    i, n   = 0, len(s)
    selects: List[int] = []
    froms:   List[int] = []
    while i < n:
        c = s[i]
        if in_str:
            if c == "'":
                if i + 1 < n and s[i + 1] == "'":
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if c == "'":
            in_str = True
            i += 1
            continue
        if c == '(':
            depth += 1
            i += 1
            continue
        if c == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0:
            m = re.match(r'(SELECT|FROM)\b', s[i:], re.IGNORECASE)
            if m:
                (selects if m.group(1).upper() == "SELECT" else froms).append(i)
                i += len(m.group(1))
                continue
        i += 1

    if not selects or not froms:
        return []
    sel = selects[-1]                                  # finaler oberster SELECT
    froms_after = [p for p in froms if p > sel]
    if not froms_after:
        return []
    col_text = s[sel + len("SELECT"):froms_after[0]]

    result: List[str] = []
    for expr in _split_top_level_commas(col_text):
        e = expr.strip()
        if not e:
            continue
        # ' AS alias' am Ende? sonst letztes Bezeichner-Token (a.b -> b)
        m = re.search(r'\bAS\s+(`[^`]+`|"[^"]+"|\[[^\]]+\]|\w+)\s*$', e, re.IGNORECASE | re.DOTALL)
        if not m:
            m = re.search(r'(`[^`]+`|"[^"]+"|\[[^\]]+\]|\w+)\s*$', e, re.DOTALL)
        if not m:
            return []                                  # unsicher -> neu erstellen
        result.append(m.group(1).strip('`"[]'))
    return result


def _convert_tsql_convert(sql: str, warnings: List[str]) -> str:
    """T-SQL ``CONVERT(typ, ausdruck)`` → MySQL ``CAST(ausdruck AS typ)``.

    Die Argument-Reihenfolge ist zwischen T-SQL (typ zuerst) und MySQL
    (ausdruck zuerst) vertauscht - unuebersetzt entsteht gueltig
    aussehendes, aber falsches SQL. Die 3-Argument-Form (Style-Nummer,
    z.B. CONVERT(VARCHAR, datum, 104)) ist Datums-/Zahlformatierung und
    muss manuell zu DATE_FORMAT() werden → Warnung, bleibt stehen.
    """
    pattern = re.compile(r'\bCONVERT\s*\(', re.IGNORECASE)
    result: List[str] = []
    i = 0
    while True:
        m = pattern.search(sql, i)
        if not m:
            result.append(sql[i:])
            break
        open_pos  = m.end() - 1
        close_pos = _paren_close(sql, open_pos)
        args      = [a.strip() for a in _split_top_level_commas(sql[m.end():close_pos])]
        result.append(sql[i:m.start()])

        converted = False
        if len(args) == 2:
            tm = re.match(r'([A-Za-z_]\w*)\s*(\(\s*\d+(?:\s*,\s*\d+)?\s*\))?$', args[0])
            mapped = _CAST_TYPE_MAP.get(tm.group(1).upper()) if tm else None
            if mapped:
                length = "" if mapped in _CAST_NO_LENGTH else (tm.group(2) or "")
                expr   = _convert_tsql_convert(args[1], warnings)   # geschachtelte CONVERTs
                result.append(f"CAST({expr} AS {mapped}{length})")
                converted = True
            else:
                warnings.append(
                    f"CONVERT() mit nicht abbildbarem Zieltyp '{args[0]}' - manuell pruefen"
                )
        elif len(args) == 3:
            warnings.append(
                "CONVERT() mit Style-Argument (Datums-/Zahlformat) - "
                "manuell zu DATE_FORMAT() umbauen, bleibt unuebersetzt im SQL"
            )
        else:
            warnings.append("CONVERT() konnte nicht geparst werden - manuell pruefen")

        if not converted:
            result.append(sql[m.start():close_pos + 1])
        i = close_pos + 1
    return ''.join(result)


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


def _convert_apply_to_join(sql: str, warnings: Optional[List[str]] = None) -> str:
    """Konvertiert OUTER/CROSS APPLY zu LEFT JOIN / JOIN mit gruppierter Subquery.

    OUTER APPLY → LEFT JOIN (subquery + GROUP BY) ON join_condition
    CROSS APPLY → JOIN      (subquery + GROUP BY) ON join_condition

    Vermeidet LATERAL, das auf manchen MariaDB-Builds nicht verfügbar ist.
    Wird die Korrelation nicht erkannt, entsteht ein Fallback-JOIN mit
    ``ON 1=1`` + Warnung (LEFT JOIN ohne ON waere ein Syntaxfehler).
    """
    if warnings is None:
        warnings = []
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

        # Fallback: Korrelation nicht erkannt - ON 1=1 haelt das SQL gueltig
        # (LEFT JOIN ohne ON ist in MySQL ein Syntaxfehler), Semantik des
        # urspruenglichen APPLY muss aber manuell geprueft werden.
        warnings.append(
            f"{m.group(1).upper()} APPLY konnte nicht vollstaendig konvertiert "
            f"werden (Korrelationsbedingung nicht erkannt) - Fallback-JOIN mit "
            f"ON 1=1, Semantik manuell pruefen!"
        )
        result.append(f'{join_kw} ({body}) AS {alias} ON 1=1')
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
    # LEN() zaehlt in T-SQL ZEICHEN - MySQL LENGTH() zaehlt Bytes (utf8mb4:
    # Umlaute = 2 Bytes!). CHAR_LENGTH() ist die korrekte Entsprechung.
    # Verbleibende Abweichung: T-SQL LEN ignoriert nachgestellte Leerzeichen.
    sql = re.sub(r'\bLEN\s*\(',              'CHAR_LENGTH(',    sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCHARINDEX\s*\(([^,]+),([^)]+)\)',
                 r'LOCATE(\1,\2)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bSUBSTRING\s*\(',       'SUBSTRING(',      sql, flags=re.IGNORECASE)

    # T-SQL CONVERT(typ, expr) → CAST(expr AS typ)  (vertauschte Argumente!)
    sql = _convert_tsql_convert(sql, warnings)

    # WITH (NOLOCK) entfernen
    sql = re.sub(r'\bWITH\s*\(\s*NOLOCK\s*\)', '', sql, flags=re.IGNORECASE)

    # ── TOP n → LIMIT n ───────────────────────────────────────────────────
    # Nur der eindeutige Fall wird uebersetzt: genau EIN "SELECT TOP n" im
    # aeussersten SELECT (= erstes SELECT des Bodys) → LIMIT n am View-Ende.
    # Alles andere (TOP in Subquery, mehrere TOPs, WITH TIES, PERCENT,
    # TOP (ausdruck)) wird gewarnt statt still gestrichen - ersatzloses
    # Entfernen aendert die Ergebnismenge!
    top_matches = list(re.finditer(
        r'\bTOP\s+(?:(\d+)\b|\(\s*[^)]*\s*\))(\s+PERCENT)?(\s+WITH\s+TIES)?',
        sql, flags=re.IGNORECASE))
    if top_matches:
        first_select = re.search(r'\bSELECT\b', sql, flags=re.IGNORECASE)
        m = top_matches[0]
        simple = (
            len(top_matches) == 1
            and m.group(1) is not None            # TOP <zahl>, kein (expr)
            and not m.group(2) and not m.group(3) # kein PERCENT / WITH TIES
            and first_select is not None
            # TOP folgt direkt auf das erste SELECT (aeusserste Ebene)
            and sql[first_select.end():m.start()].strip() == ""
        )
        if simple:
            limit_n = m.group(1)
            sql = sql[:m.start()] + sql[m.end():]
            sql = sql.rstrip().rstrip(';')
            sql += f"\nLIMIT {limit_n}"
            warnings.append(
                f"TOP {limit_n} wurde zu LIMIT {limit_n} am View-Ende - "
                f"Ergebnis pruefen (ORDER BY-Bezug!)"
            )
        else:
            warnings.append(
                "TOP-Klausel konnte nicht automatisch uebersetzt werden "
                "(Subquery/mehrfach/PERCENT/WITH TIES/Ausdruck) - "
                "manuell in LIMIT umbauen! TOP bleibt im SQL stehen."
            )

    # ── T-SQL String-Konkatenation (+) → MySQL CONCAT() ───────────────────
    sql = _convert_string_concat(sql)

    # Verbleibende '+' neben String-Literalen = nicht erkannte Kette
    # (z.B. CAST mit 2 Klammer-Ebenen). In MySQL rechnet '+' NUMERISCH -
    # 'text' + x ergibt still 0/Unsinn statt Konkatenation → warnen!
    if re.search(r"'[^']*'\s*\+|\+\s*'[^']*'", sql):
        warnings.append(
            "String-Konkatenation mit '+' konnte nicht vollstaendig zu "
            "CONCAT() konvertiert werden - MySQL wuerde numerisch rechnen! "
            "Betroffene Stellen manuell auf CONCAT() umbauen."
        )

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
    sql = _convert_apply_to_join(sql, warnings)

    # ── Erkennungs-Pass: bekannte, NICHT automatisch übersetzbare Konstrukte ─
    # Diese Funktionen/Klauseln haben in MySQL keine 1:1-Entsprechung oder
    # eine abweichende Argument-Semantik. Sie bleiben unverändert im SQL
    # stehen und werden als Warnung geflaggt (landet als -- ⚠ Kommentar im
    # DDL), statt still durchzulaufen und erst beim Deploy zu knallen.
    _UNSUPPORTED = [
        (r'\bDATEADD\s*\(',      "DATEADD() – MySQL: DATE_ADD(datum, INTERVAL n einheit); manuell umbauen"),
        (r'\bDATEDIFF\s*\(',     "DATEDIFF() – Argument-Semantik weicht ab (T-SQL: einheit,von,bis / MySQL: bis,von in Tagen); manuell umbauen"),
        (r'\bDATEPART\s*\(',     "DATEPART() – MySQL: EXTRACT(einheit FROM datum); manuell umbauen"),
        (r'\bDATENAME\s*\(',     "DATENAME() – MySQL: DATE_FORMAT()/MONTHNAME(); manuell umbauen"),
        (r'\bFORMAT\s*\(',       "FORMAT() – T-SQL-.NET-Formatstrings; MySQL: DATE_FORMAT()/FORMAT(zahl,stellen); manuell pruefen"),
        (r'\bFULL\s+OUTER\s+JOIN\b', "FULL OUTER JOIN – von MySQL nicht unterstuetzt; per LEFT JOIN UNION RIGHT JOIN nachbauen"),
        (r'\bPIVOT\s*\(',        "PIVOT – von MySQL nicht unterstuetzt; per CASE WHEN + GROUP BY nachbauen"),
        (r'\bUNPIVOT\s*\(',      "UNPIVOT – von MySQL nicht unterstuetzt; per UNION ALL nachbauen"),
        (r'\bOPENJSON\s*\(',     "OPENJSON() – MySQL: JSON_TABLE() (ab 8.0/MariaDB 10.6); manuell umbauen"),
        (r'\bSTUFF\s*\(',        "STUFF() – MySQL: INSERT(str,pos,len,neu); manuell umbauen"),
        (r'\bPATINDEX\s*\(',     "PATINDEX() – MySQL: REGEXP_INSTR(); manuell umbauen"),
    ]
    for pattern, hint in _UNSUPPORTED:
        if re.search(pattern, sql, flags=re.IGNORECASE):
            warnings.append(f"Nicht automatisch uebersetzbar: {hint}")

    # CTE-Hinweis (WITH … AS (…)): erst ab MySQL 8.0 / MariaDB 10.2 verfuegbar
    if re.search(r'(?:^|\n)\s*WITH\b.*?\bAS\s*\(', sql, flags=re.IGNORECASE | re.DOTALL):
        warnings.append("CTE (WITH … AS) verwendet – erfordert MySQL >= 8.0 / MariaDB >= 10.2")

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
#  Gefilterte (partielle) Indexe - MySQL kennt kein WHERE bei CREATE INDEX
# ════════════════════════════════════════════════════════════════════════════
_SIMPLE_EQ_FILTER_RE = re.compile(r'^\(*\[([^\]]+)\]\s*=\s*\(*([^()]+)\)*$')


def _parse_simple_equality_filter(filter_def: Optional[str]) -> Optional[Tuple[str, str]]:
    """Erkennt SQL-Server-Filterausdrücke der Form '([IsDefault]=(1))'.

    Gibt (Spaltenname, Wert) zurück, oder None wenn das Muster nicht
    (eindeutig) erkannt wird - dann muss der Index manuell nachgebaut werden.
    """
    if not filter_def:
        return None
    m = _SIMPLE_EQ_FILTER_RE.match(filter_def.strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def render_index_ddl(table_name: str, idx: dict) -> List[str]:
    """Erzeugt die DDL-Zeile(n) für einen Index, inkl. Emulation gefilterter
    (partieller) UNIQUE-Indexe.

    MySQL/MariaDB kennt kein `CREATE INDEX ... WHERE ...`. Ein gefilterter
    SQL-Server-UNIQUE-Index der Form `WHERE [Spalte] = <Wert>` wird daher über
    eine generierte (virtuelle) Spalte emuliert: Die Spalte liefert die
    Indexwerte nur für Zeilen, die den Filter erfüllen, sonst NULL - und
    MySQL UNIQUE erlaubt beliebig viele NULLs, womit nur die gefilterten
    Zeilen tatsächlich eindeutig sein müssen (= identisches Verhalten).

    Kann der Filter nicht erkannt werden, wird eine Warnung ausgegeben statt
    eines (möglicherweise falschen) unkonditionalen UNIQUE-Index.
    """
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', idx["name"])
    unique_kw = "UNIQUE " if idx["unique"] else ""
    col_list  = ", ".join(
        f"{mssql_name(c['name'])} {'DESC' if c['desc'] else 'ASC'}"
        for c in idx["columns"]
    )
    lines: List[str] = []

    if idx.get("filter"):
        parsed = _parse_simple_equality_filter(idx["filter"])
        if parsed and idx["unique"]:
            filter_col, filter_val = parsed
            key_col = f"_{safe_name}_key"
            if len(idx["columns"]) == 1:
                source_expr = mssql_name(idx["columns"][0]["name"])
            else:
                source_expr = "CONCAT_WS('|', " + ", ".join(
                    mssql_name(c["name"]) for c in idx["columns"]
                ) + ")"
            lines.append(
                f"-- Gefilterter Index (urspr. WHERE [{filter_col}] = {filter_val}) "
                f"-> als generierte Spalte emuliert (MySQL kennt keine partiellen Indexe)"
            )
            lines.append(
                f"ALTER TABLE {mssql_name(table_name)} ADD COLUMN `{key_col}` "
                f"VARCHAR(255) GENERATED ALWAYS AS "
                f"(IF({mssql_name(filter_col)} = {filter_val}, {source_expr}, NULL)) VIRTUAL;"
            )
            lines.append(
                f"CREATE {unique_kw}INDEX `{safe_name}` ON {mssql_name(table_name)} (`{key_col}`);"
            )
            return lines
        else:
            lines.append(
                f"-- ⚠ Gefilterter Index (urspr. WHERE {idx['filter']}) konnte nicht "
                f"automatisch übersetzt werden - MySQL kennt keine partiellen Indexe. "
                f"Manuell prüfen!"
            )

    lines.append(
        f"CREATE {unique_kw}INDEX `{safe_name}` ON {mssql_name(table_name)} ({col_list});"
    )
    return lines


# ════════════════════════════════════════════════════════════════════════════
#  Foreign-Key-Referenzaktionen (ON DELETE / ON UPDATE)
# ════════════════════════════════════════════════════════════════════════════
# SQL-Server-Aktionsnamen (sys.foreign_keys.*_referential_action_desc) →
# MySQL-Klausel. NO_ACTION wird weggelassen (MySQL-Default RESTRICT verhaelt
# sich identisch). SET_DEFAULT kennt InnoDB nicht → Warnkommentar statt Klausel.
_FK_ACTION_SQL = {
    "CASCADE":     "CASCADE",
    "SET_NULL":    "SET NULL",
    "SET NULL":    "SET NULL",
}


def fk_col_list(cols: List[str]) -> str:
    """FK-Spaltenliste: ['A', 'B'] → '`A`, `B`' (Composite-FK-Unterstützung)."""
    return ", ".join(mssql_name(c) for c in cols)


def fk_actions_sql(fk: dict) -> Tuple[str, List[str]]:
    """Erzeugt die ON DELETE/ON UPDATE-Klauseln eines FK-Eintrags.

    Returns
    -------
    (suffix, warnings)
        suffix    – z.B. " ON DELETE CASCADE" (leer wenn beide NO_ACTION)
        warnings  – Hinweise fuer nicht abbildbare Aktionen (SET_DEFAULT)
    """
    parts: List[str] = []
    warns: List[str] = []
    for key, clause in (("on_delete", "ON DELETE"), ("on_update", "ON UPDATE")):
        action = (fk.get(key) or "NO_ACTION").upper()
        if action in ("NO_ACTION", "NO ACTION", "RESTRICT"):
            continue
        mapped = _FK_ACTION_SQL.get(action)
        if mapped:
            parts.append(f"{clause} {mapped}")
        else:
            warns.append(
                f"FK {fk['name']}: {clause} {action} wird von MySQL/InnoDB "
                f"nicht unterstuetzt - Klausel weggelassen, manuell pruefen!"
            )
    suffix = (" " + " ".join(parts)) if parts else ""
    return suffix, warns


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

        # Berechnete Spalten (MSSQL computed columns): werden als normale
        # Spalten angelegt (haelt die Datenmigration konsistent), aber die
        # Formel geht verloren - Werte aktualisieren sich nicht mehr!
        computed = [c["name"] for c in tinfo["columns"] if c.get("computed")]
        for cname in computed:
            lines.append(
                f"-- ⚠ {tname}.{cname} ist in MSSQL eine BERECHNETE Spalte - "
                f"wird als normale Spalte angelegt, Formel nicht uebernommen. "
                f"Werte veralten bei Aenderungen! Ggf. als GENERATED ALWAYS AS "
                f"nachbauen."
            )

        # datetimeoffset -> DATETIME: MySQL kennt keinen Zeitzonen-Offset,
        # der Offset geht verloren. Werte ggf. vorher nach UTC normalisieren.
        tz_cols = [c["name"] for c in tinfo["columns"]
                   if c["type"].lower() == "datetimeoffset"]
        for cname in tz_cols:
            lines.append(
                f"-- ⚠ {tname}.{cname}: datetimeoffset -> DATETIME - der "
                f"Zeitzonen-Offset geht verloren! Werte ggf. vor der Migration "
                f"nach UTC normalisieren."
            )

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
            actions, fk_warns = fk_actions_sql(fk)
            for w in fk_warns:
                lines.append(f"-- ⚠ {w}")
            lines.append(
                f"ALTER TABLE {mssql_name(tinfo['name'])} "
                f"ADD CONSTRAINT `{safe_fk}` "
                f"FOREIGN KEY ({fk_col_list(fk['from_cols'])}) "
                f"REFERENCES {mssql_name(fk['to_table'])} ({fk_col_list(fk['to_cols'])})"
                f"{actions};"
            )
    if any(tinfo["fk"] for tinfo in schema["tables"].values()):
        lines.append("")

    lines.append("SET FOREIGN_KEY_CHECKS = 1;")
    lines.append("")

    # ── Indexes (UNIQUE und Non-Clustered) ────────────────────────────────
    has_indexes = any(tinfo.get("indexes") for tinfo in schema["tables"].values())
    if has_indexes:
        lines.append("-- Indexes")
    for tinfo in schema["tables"].values():
        for idx in tinfo.get("indexes", []):
            lines.extend(render_index_ddl(tinfo["name"], idx))
    if has_indexes:
        lines.append("")

    # ── Views (topologisch sortiert) ──────────────────────────────────────
    for vinfo in _topo_sort_views(schema["views"]):
        vname        = vinfo["name"]
        if vname.lower() in EXCLUDED_VIEWS:
            continue
        vdef, warns  = convert_view_sql(vinfo["definition"])
        lines.append(f"DROP VIEW IF EXISTS {mssql_name(vname)};")
        for w in warns:
            lines.append(f"-- ⚠ {w}")
        lines.append(f"CREATE VIEW {mssql_name(vname)} AS")
        lines.append(vdef + ";")
        lines.append("")

    return "\n".join(lines)

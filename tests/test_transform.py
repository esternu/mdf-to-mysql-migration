"""
Tests für src/transform.py
Deckt ab: Typkonvertierung, Bezeichner-Mapping, Default-Konvertierung,
           View-SQL-Konvertierung, topologischer Sort und DDL-Generierung.
Keine externen Abhängigkeiten – keine Mocks erforderlich.
"""
import re
import pytest
from transform import (
    convert_type,
    mssql_name,
    convert_default,
    convert_view_sql,
    generate_mysql_ddl,
    _topo_sort_views,
    _convert_string_concat,
    render_index_ddl,
    _parse_simple_equality_filter,
)


# ════════════════════════════════════════════════════════════════════════════
#  convert_type
# ════════════════════════════════════════════════════════════════════════════
class TestConvertType:
    # VARCHAR-Varianten
    def test_nvarchar_normal(self):
        assert convert_type("nvarchar", 100, None, None) == "VARCHAR(100)"

    def test_nvarchar_max_returns_longtext(self):
        assert convert_type("nvarchar", -1, None, None) == "LONGTEXT"

    def test_varchar_at_utf8mb4_boundary(self):
        assert convert_type("varchar", 16383, None, None) == "VARCHAR(16383)"

    def test_varchar_over_boundary_returns_text(self):
        assert convert_type("varchar", 16384, None, None) == "TEXT"

    def test_varchar_very_large_returns_mediumtext(self):
        assert convert_type("varchar", 100_000, None, None) == "MEDIUMTEXT"

    def test_varchar_max_returns_longtext(self):
        assert convert_type("varchar", -1, None, None) == "LONGTEXT"

    # CHAR
    def test_char_normal(self):
        assert convert_type("char", 10, None, None) == "CHAR(10)"

    def test_char_clamped_to_255(self):
        assert convert_type("char", 300, None, None) == "CHAR(255)"

    # Numerische Typen
    def test_money(self):
        assert convert_type("money", None, None, None) == "DECIMAL(19,4)"

    def test_smallmoney(self):
        assert convert_type("smallmoney", None, None, None) == "DECIMAL(10,4)"

    def test_bit(self):
        assert convert_type("bit", None, None, None) == "TINYINT(1)"

    def test_tinyint_unsigned(self):
        # SQL Server TINYINT ist 0-255 (unsigned), MySQL TINYINT ist
        # standardmaessig signed -> UNSIGNED noetig fuer korrekten Wertebereich
        assert convert_type("tinyint", None, None, None) == "TINYINT UNSIGNED"

    def test_decimal_with_scale(self):
        assert convert_type("decimal", None, 10, 2) == "DECIMAL(10,2)"

    def test_decimal_zero_scale(self):
        assert convert_type("decimal", None, 8, 0) == "DECIMAL(8,0)"

    def test_numeric_alias(self):
        assert convert_type("numeric", None, 5, 3) == "DECIMAL(5,3)"

    def test_float(self):
        assert convert_type("float", None, None, None) == "DOUBLE"

    def test_real(self):
        assert convert_type("real", None, None, None) == "FLOAT"

    # Datum/Zeit
    def test_datetime(self):
        assert convert_type("datetime", None, None, None) == "DATETIME"

    def test_datetime2(self):
        assert convert_type("datetime2", None, None, None) == "DATETIME(6)"

    # ── datetime2/time-Praezision (TODO 2.6) ─────────────────────────────
    def test_datetime2_scale_zero(self):
        # datetime2(0) (Cockpit-Standard) -> DATETIME ohne fsp, nicht (6)
        assert convert_type("datetime2", None, None, 0) == "DATETIME"

    def test_datetime2_scale_preserved(self):
        assert convert_type("datetime2", None, None, 3) == "DATETIME(3)"

    def test_datetime2_scale_capped_at_6(self):
        # SQL Server erlaubt 7, MySQL max. 6
        assert convert_type("datetime2", None, None, 7) == "DATETIME(6)"

    def test_time_scale_preserved(self):
        assert convert_type("time", None, None, 3) == "TIME(3)"

    def test_time_scale_zero(self):
        assert convert_type("time", None, None, 0) == "TIME"

    def test_datetimeoffset_scale(self):
        assert convert_type("datetimeoffset", None, None, 2) == "DATETIME(2)"

    def test_date(self):
        assert convert_type("date", None, None, None) == "DATE"

    # ── timestamp/rowversion ist KEINE Uhrzeit -> BINARY(8) (#63) ─────────
    def test_timestamp_is_binary_not_time(self):
        # SQL Server timestamp = rowversion (8-Byte-Zaehler), keine Uhrzeit.
        assert convert_type("timestamp", None, None, None) == "BINARY(8)"

    def test_rowversion_is_binary(self):
        assert convert_type("rowversion", None, None, None) == "BINARY(8)"

    # Sonstige
    def test_uniqueidentifier(self):
        assert convert_type("uniqueidentifier", None, None, None) == "CHAR(36)"

    def test_varbinary(self):
        assert convert_type("varbinary", None, None, None) == "LONGBLOB"

    # ── binary/varbinary mit Laenge (TODO 2.5) ───────────────────────────
    def test_varbinary_max_is_longblob(self):
        assert convert_type("varbinary", -1, None, None) == "LONGBLOB"

    def test_binary_keeps_length(self):
        # binary(16) darf NICHT zu BINARY(1) degradieren (Trunkierung!)
        assert convert_type("binary", 16, None, None) == "BINARY(16)"

    def test_binary_length_capped_at_255(self):
        assert convert_type("binary", 400, None, None) == "BINARY(255)"

    def test_varbinary_keeps_length(self):
        assert convert_type("varbinary", 128, None, None) == "VARBINARY(128)"

    def test_varbinary_huge_becomes_longblob(self):
        assert convert_type("varbinary", 70000, None, None) == "LONGBLOB"

    def test_xml(self):
        assert convert_type("xml", None, None, None) == "LONGTEXT"

    def test_unknown_type_uppercased(self):
        assert convert_type("MYTYPE", None, None, None) == "MYTYPE"

    def test_case_insensitive(self):
        assert convert_type("NVARCHAR", 50, None, None) == "VARCHAR(50)"


# ════════════════════════════════════════════════════════════════════════════
#  mssql_name
# ════════════════════════════════════════════════════════════════════════════
class TestMssqlName:
    def test_brackets_converted(self):
        assert mssql_name("[MyColumn]") == "`MyColumn`"

    def test_plain_name_wrapped(self):
        assert mssql_name("MyColumn") == "`MyColumn`"

    def test_name_with_spaces(self):
        assert mssql_name("[My Column]") == "`My Column`"

    def test_empty_brackets(self):
        assert mssql_name("[]") == "``"


# ════════════════════════════════════════════════════════════════════════════
#  convert_default
# ════════════════════════════════════════════════════════════════════════════
class TestConvertDefault:
    def test_none_input(self):
        assert convert_default(None) is None

    def test_getdate(self):
        assert convert_default("(getdate())") == "CURRENT_TIMESTAMP"

    def test_getutcdate(self):
        assert convert_default("(getutcdate())") == "CURRENT_TIMESTAMP"

    def test_newid_returns_none(self):
        assert convert_default("(newid())") is None

    def test_one(self):
        assert convert_default("((1))") == "'1'"

    def test_zero(self):
        assert convert_default("((0))") == "'0'"

    def test_string_literal(self):
        assert convert_default("('hello')") == "'hello'"

    def test_empty_string_returns_none(self):
        assert convert_default("('')") is None

    # ── Robustheit (TODO 2.7) ────────────────────────────────────────────
    def test_unicode_literal_prefix_stripped(self):
        # (N'Standard') erzeugte vorher das kaputte Literal 'N'Standard''
        assert convert_default("(N'Standard')") == "'Standard'"

    def test_inner_parens_preserved(self):
        # zeichenweises strip("()") frass die schliessende Klammer im Wert
        assert convert_default("('Wert (intern)')") == "'Wert (intern)'"

    def test_escaped_quote_in_literal(self):
        assert convert_default("('it''s')") == "'it''s'"

    def test_getdate_with_fsp_matching_column(self):
        # Striktes MySQL 8: DATETIME(6)-Spalte braucht CURRENT_TIMESTAMP(6)
        assert convert_default("(getdate())", "DATETIME(6)") == "CURRENT_TIMESTAMP(6)"

    def test_getdate_plain_datetime_no_fsp(self):
        assert convert_default("(getdate())", "DATETIME") == "CURRENT_TIMESTAMP"

    def test_negative_number_default(self):
        assert convert_default("((-1))") == "'-1'"


# ════════════════════════════════════════════════════════════════════════════
#  _convert_string_concat
# ════════════════════════════════════════════════════════════════════════════
class TestConvertStringConcat:
    """Unit-Tests für die T-SQL → CONCAT()-Konvertierung.

    Hintergrund: T-SQL benutzt ``+`` als String-Operator.  In MySQL/MariaDB
    ist ``+`` nur arithmetisch; String-Verkettung erfordert ``CONCAT()``.
    Die Funktion wandelt Ketten um, die mindestens ein String-Literal enthalten.
    """

    # ── Grundlegende Konvertierungen ─────────────────────────────────────
    def test_backtick_plus_string_literal(self):
        sql    = "`Name` + ' suffix'"
        result = _convert_string_concat(sql)
        assert result == "CONCAT(`Name`, ' suffix')"

    def test_string_literal_plus_backtick(self):
        sql    = "'prefix ' + `Name`"
        result = _convert_string_concat(sql)
        assert result == "CONCAT('prefix ', `Name`)"

    def test_two_string_literals(self):
        result = _convert_string_concat("'hello' + ' world'")
        assert result == "CONCAT('hello', ' world')"

    def test_three_part_chain(self):
        sql    = "`col` + ' (' + `id` + ')'"
        # `id` ist kein String-Literal und kein CAST → kein Operand
        # → nur `col` + ' (' wird als Kette erkannt; `id` + ')' ebenfalls
        # Hauptsache: Ergebnis enthält CONCAT und kein rohes +
        result = _convert_string_concat(sql)
        assert "CONCAT" in result

    # ── CAST mit verschachtelten Klammern ────────────────────────────────
    def test_backtick_plus_cast_char_plus_string(self):
        """Kernfall aus ViewSurface / ViewSurfaceArticle."""
        sql    = "`Jig Name` + ' (' + CAST(`Jig Id` AS CHAR(100)) + ')'"
        result = _convert_string_concat(sql)
        assert result == "CONCAT(`Jig Name`, ' (', CAST(`Jig Id` AS CHAR(100)), ')')"

    def test_cast_char_preserved_correctly(self):
        """CAST(x AS CHAR(n)) – das innere n) darf nicht als Kettenende gelten."""
        sql    = "`col` + ' ' + CAST(`id` AS CHAR(50))"
        result = _convert_string_concat(sql)
        assert "CAST(`id` AS CHAR(50))" in result
        assert "CONCAT" in result

    def test_full_group_concat_chain(self):
        """GROUP_CONCAT-Kontext: nur die + -Kette wird umgeschrieben,
        ORDER BY / SEPARATOR bleiben unangetastet."""
        sql = (
            "GROUP_CONCAT(`Article Name` + ' (' + "
            "CAST(`Article Index` AS CHAR(100)) + ')' "
            "ORDER BY `Article Name` SEPARATOR ', ')"
        )
        result = _convert_string_concat(sql)
        assert "CONCAT(`Article Name`, ' (', CAST(`Article Index` AS CHAR(100)), ')')" in result
        assert "ORDER BY `Article Name`" in result
        assert "SEPARATOR ', '" in result
        assert "GROUP_CONCAT(" in result

    # ── Kein Eingriff bei reiner Arithmetik ─────────────────────────────
    def test_arithmetic_plain_identifiers_unchanged(self):
        sql    = "surface_si + jig_surface_si"
        assert _convert_string_concat(sql) == sql

    def test_backtick_plus_backtick_no_string_unchanged(self):
        """Zwei Backtick-Bezeichner ohne String-Literal → bleibt arithmetisch."""
        sql    = "`col_a` + `col_b`"
        assert _convert_string_concat(sql) == sql

    def test_multiplication_unchanged(self):
        sql    = "`surface` * `factor`"
        assert _convert_string_concat(sql) == sql

    def test_empty_string_literal_converted(self):
        """Leeres String-Literal '' gilt als String-Kontext."""
        sql    = "`col` + ''"
        result = _convert_string_concat(sql)
        assert "CONCAT" in result

    # ── Integration: convert_view_sql ruft _convert_string_concat auf ───
    def test_convert_view_sql_applies_concat_conversion(self):
        """convert_view_sql muss _convert_string_concat intern aufrufen."""
        tsql = "SELECT `col` + ' suffix' AS label FROM t"
        result, _ = convert_view_sql(tsql)
        assert "CONCAT(`col`, ' suffix')" in result
        # Sicherstellen dass kein + string übrig bleibt
        assert "+ ' suffix'" not in result

    def test_convert_view_sql_cast_chain_in_view(self):
        """Vollständiger T-SQL-View-Header + CAST-Kette werden korrekt konvertiert."""
        tsql = (
            "CREATE VIEW [dbo].[V] AS\n"
            "SELECT [Jig Name] + ' (' + CAST([Jig Id] AS CHAR(10)) + ')' AS label\n"
            "FROM [dbo].[T]"
        )
        result, _ = convert_view_sql(tsql)
        assert "CONCAT(" in result
        assert "CREATE VIEW" not in result   # header entfernt
        assert "[dbo]" not in result          # schema-prefix entfernt


# ════════════════════════════════════════════════════════════════════════════
#  convert_view_sql
# ════════════════════════════════════════════════════════════════════════════
class TestConvertViewSql:
    # Header-Entfernung
    def test_removes_create_view_header(self):
        sql = "CREATE VIEW [dbo].[MyView] AS\nSELECT 1 AS val"
        result, _ = convert_view_sql(sql)
        assert "CREATE VIEW" not in result
        assert "SELECT 1 AS val" in result

    def test_removes_dbo_bracket_prefix(self):
        sql = "SELECT * FROM [dbo].[MyTable]"
        result, _ = convert_view_sql(sql)
        assert "[dbo]" not in result
        assert "dbo." not in result

    def test_removes_plain_dbo_prefix(self):
        sql = "SELECT * FROM dbo.MyTable"
        result, _ = convert_view_sql(sql)
        assert "dbo." not in result

    # Bezeichner
    def test_brackets_to_backticks(self):
        sql = "SELECT [col] FROM [tbl]"
        result, _ = convert_view_sql(sql)
        assert "[" not in result
        assert "`col`" in result
        assert "`tbl`" in result

    # Typ-Ersetzungen
    def test_money_replaced(self):
        result, _ = convert_view_sql("SELECT CAST(x AS money)")
        assert "DECIMAL(19,4)" in result
        assert "money" not in result

    def test_bit_replaced(self):
        result, _ = convert_view_sql("SELECT CAST(x AS bit)")
        assert "TINYINT(1)" in result

    # Funktionen
    def test_getdate_to_now(self):
        result, _ = convert_view_sql("SELECT GETDATE()")
        assert "NOW()" in result
        assert "GETDATE" not in result

    def test_getutcdate_to_utc_timestamp(self):
        result, _ = convert_view_sql("SELECT GETUTCDATE()")
        assert "UTC_TIMESTAMP()" in result

    def test_isnull_to_ifnull(self):
        result, _ = convert_view_sql("SELECT ISNULL(col, 0)")
        assert "IFNULL(" in result
        assert "ISNULL" not in result

    def test_iif_to_if(self):
        result, _ = convert_view_sql("SELECT IIF(a > 0, 'yes', 'no')")
        assert result.startswith("SELECT IF(")

    def test_len_to_char_length(self):
        # TODO 1.3: LEN() zaehlt Zeichen -> CHAR_LENGTH(), nicht LENGTH()
        # (LENGTH zaehlt Bytes; utf8mb4-Umlaute waeren doppelt gezaehlt).
        result, _ = convert_view_sql("SELECT LEN(col)")
        assert "CHAR_LENGTH(col)" in result
        assert not re.search(r"(?<!CHAR_)LENGTH\(", result)

    def test_char_length_not_double_converted(self):
        # Bereits korrektes CHAR_LENGTH darf nicht angefasst werden.
        result, _ = convert_view_sql("SELECT CHAR_LENGTH(col)")
        assert result.count("CHAR_LENGTH(") == 1

    # Entfernte Konstrukte
    def test_nolock_removed(self):
        result, _ = convert_view_sql("SELECT * FROM t WITH (NOLOCK)")
        assert "NOLOCK" not in result

    def test_top_translated_to_limit(self):
        # TODO 2.2: TOP n darf nicht ersatzlos verschwinden - LIMIT n am Ende.
        result, warns = convert_view_sql("SELECT TOP 10 col FROM t")
        assert "TOP" not in result
        assert result.rstrip().endswith("LIMIT 10")
        assert any("LIMIT 10" in w for w in warns)

    def test_top_in_subquery_warns_and_keeps_sql(self):
        sql = "SELECT a FROM (SELECT TOP 5 a FROM t ORDER BY a) AS s"
        result, warns = convert_view_sql(sql)
        assert "TOP 5" in result            # bleibt stehen statt still zu verschwinden
        assert any("manuell" in w for w in warns)

    def test_top_percent_warns(self):
        result, warns = convert_view_sql("SELECT TOP 10 PERCENT col FROM t")
        assert any("PERCENT" in w or "manuell" in w for w in warns)
        assert "LIMIT" not in result

    def test_top_with_expression_warns(self):
        result, warns = convert_view_sql("SELECT TOP (@n) col FROM t")
        assert any("manuell" in w for w in warns)
        assert "LIMIT" not in result

    def test_no_top_no_limit(self):
        result, warns = convert_view_sql("SELECT col FROM t")
        assert "LIMIT" not in result
        assert warns == []

    # STRING_AGG → GROUP_CONCAT
    def test_string_agg_with_order(self):
        sql = "SELECT STRING_AGG(Name, ', ') WITHIN GROUP (ORDER BY Name)"
        result, _ = convert_view_sql(sql)
        assert "GROUP_CONCAT" in result
        assert "STRING_AGG" not in result
        assert "ORDER BY" in result
        assert "SEPARATOR" in result

    def test_string_agg_separator_preserved(self):
        sql = "SELECT STRING_AGG(col, '; ') WITHIN GROUP (ORDER BY col)"
        result, _ = convert_view_sql(sql)
        assert "SEPARATOR '; '" in result

    # OUTER/CROSS APPLY → JOIN
    def test_outer_apply_to_left_join(self):
        sql = (
            "SELECT t.Id\n"
            "FROM t\n"
            "OUTER APPLY (\n"
            "    SELECT SUM(s.Val) AS Total, s.TId\n"
            "    FROM sub s\n"
            "    WHERE s.TId = t.Id\n"
            ") agg"
        )
        result, _ = convert_view_sql(sql)
        assert "LEFT JOIN" in result
        assert "OUTER APPLY" not in result
        assert "GROUP BY" in result

    def test_cross_apply_to_join(self):
        sql = (
            "SELECT t.Id\n"
            "FROM t\n"
            "CROSS APPLY (\n"
            "    SELECT MAX(s.Val) AS MaxVal, s.TId\n"
            "    FROM sub s\n"
            "    WHERE s.TId = t.Id\n"
            ") agg"
        )
        result, _ = convert_view_sql(sql)
        assert "JOIN" in result
        assert "CROSS APPLY" not in result

    # ── APPLY-Fallback mit ON 1=1 (TODO 2.8) ─────────────────────────────
    def test_apply_fallback_has_on_clause(self):
        # Body ohne erkennbare WHERE-Korrelation -> Fallback; LEFT JOIN
        # ohne ON waere ein MySQL-Syntaxfehler.
        sql = "SELECT * FROM a OUTER APPLY (SELECT 1 AS x) AS s"
        result, warns = convert_view_sql(sql)
        assert "LEFT JOIN" in result
        assert "ON 1=1" in result
        assert any("manuell pruefen" in w for w in warns)

    def test_cross_apply_fallback_has_on_clause(self):
        sql = "SELECT * FROM a CROSS APPLY (SELECT 1 AS x) AS s"
        result, warns = convert_view_sql(sql)
        assert "ON 1=1" in result
        assert any("APPLY" in w for w in warns)

    def test_recognized_apply_correlation_no_fallback_warning(self):
        sql = ("SELECT * FROM a OUTER APPLY ("
               "SELECT SUM(x) AS sx FROM detail d WHERE d.aid = a.id) AS s")
        result, warns = convert_view_sql(sql)
        assert "ON 1=1" not in result
        assert not any("ON 1=1" in w for w in warns)

    # ── Verbleibende '+'-Konkatenation warnt (TODO 2.4) ──────────────────
    def test_unconverted_string_concat_warns(self):
        # 2 Klammer-Ebenen im CAST -> _convert_string_concat greift nicht
        sql = "SELECT 'x' + CAST(ROUND(SUM(a / b), 2) AS CHAR(10)) FROM t"
        result, warns = convert_view_sql(sql)
        if "+" in result:   # nicht konvertiert -> Warnung ist Pflicht
            assert any("CONCAT" in w and "numerisch" in w for w in warns)

    def test_plain_string_plus_column_warns_if_unconverted(self):
        result, warns = convert_view_sql("SELECT 'Wert: ' + very_long_col_expr(x, y) FROM t")
        if re.search(r"'[^']*'\s*\+", result):
            assert any("numerisch" in w for w in warns)

    def test_fully_converted_concat_produces_no_warning(self):
        result, warns = convert_view_sql("SELECT col + ' (' + 'a' + ')' FROM t")
        assert "CONCAT" in result or "+" not in result
        assert not any("numerisch" in w for w in warns)

    def test_numeric_addition_not_flagged(self):
        _, warns = convert_view_sql("SELECT a + b FROM t")
        assert not any("numerisch" in w for w in warns)

    # ── CONVERT() → CAST() (TODO 2.3) ────────────────────────────────────
    def test_convert_two_args_becomes_cast(self):
        result, warns = convert_view_sql("SELECT CONVERT(INT, col) FROM t")
        assert "CAST(col AS SIGNED)" in result
        assert "CONVERT" not in result
        assert warns == []

    def test_convert_nvarchar_with_length(self):
        # nvarchar wird vorher durch die Typ-Map zu CHAR - Laenge bleibt
        result, _ = convert_view_sql("SELECT CONVERT(NVARCHAR(10), col) FROM t")
        assert "CAST(col AS CHAR(10))" in result

    def test_convert_decimal_keeps_precision(self):
        result, _ = convert_view_sql("SELECT CONVERT(DECIMAL(18,2), col) FROM t")
        assert "CAST(col AS DECIMAL(18,2))" in result

    def test_convert_nested_expression(self):
        result, _ = convert_view_sql("SELECT CONVERT(INT, ROUND(a / b, 0)) FROM t")
        assert "CAST(ROUND(a / b, 0) AS SIGNED)" in result

    def test_convert_with_style_warns_and_keeps(self):
        # 3-Argument-Form = Datumsformatierung, nicht automatisch uebersetzbar
        result, warns = convert_view_sql("SELECT CONVERT(CHAR(10), col, 104) FROM t")
        assert "CONVERT" in result                 # bleibt stehen
        assert any("Style" in w for w in warns)

    def test_convert_unknown_type_warns(self):
        result, warns = convert_view_sql("SELECT CONVERT(GEOGRAPHY, col) FROM t")
        assert any("nicht abbildbarem Zieltyp" in w for w in warns)

    # ── Warnungen fuer nicht uebersetzbare Konstrukte (TODO 2.1) ─────────
    def test_dateadd_warns(self):
        _, warns = convert_view_sql("SELECT DATEADD(day, 1, col) FROM t")
        assert any("DATEADD" in w for w in warns)

    def test_datediff_warns(self):
        _, warns = convert_view_sql("SELECT DATEDIFF(day, a, b) FROM t")
        assert any("DATEDIFF" in w for w in warns)

    def test_full_outer_join_warns(self):
        _, warns = convert_view_sql("SELECT * FROM a FULL OUTER JOIN b ON a.id=b.id")
        assert any("FULL OUTER JOIN" in w for w in warns)

    def test_pivot_warns(self):
        _, warns = convert_view_sql("SELECT * FROM t PIVOT (SUM(x) FOR y IN ([a]))")
        assert any("PIVOT" in w for w in warns)

    def test_cte_version_hint(self):
        _, warns = convert_view_sql(
            "CREATE VIEW [dbo].[V] AS\nWITH Base AS (SELECT 1 AS n) SELECT * FROM Base")
        assert any("CTE" in w for w in warns)

    def test_clean_sql_produces_no_warnings(self):
        _, warns = convert_view_sql("SELECT a, b FROM t WHERE a > 1")
        assert warns == []

    def test_warnings_land_in_generated_ddl(self):
        schema = dict(_MINIMAL_SCHEMA)
        schema = {**schema, "views": {
            "dbo.V": {"name": "V", "schema": "dbo",
                      "definition": "CREATE VIEW [dbo].[V] AS "
                                    "SELECT DATEDIFF(day, a, b) AS d FROM t"}
        }}
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "-- ⚠" in ddl and "DATEDIFF" in ddl

    # Rückgabe-Tuple
    def test_returns_tuple_of_sql_and_warnings(self):
        result = convert_view_sql("SELECT 1")
        assert isinstance(result, tuple)
        assert len(result) == 2
        sql, warns = result
        assert isinstance(sql, str)
        assert isinstance(warns, list)


# ════════════════════════════════════════════════════════════════════════════
#  _topo_sort_views
# ════════════════════════════════════════════════════════════════════════════
class TestTopoSortViews:
    def test_single_view(self):
        views = {"dbo.A": {"name": "A", "definition": "SELECT 1"}}
        result = _topo_sort_views(views)
        assert len(result) == 1
        assert result[0]["name"] == "A"

    def test_two_independent_views_both_returned(self):
        views = {
            "dbo.A": {"name": "A", "definition": "SELECT 1"},
            "dbo.B": {"name": "B", "definition": "SELECT 2"},
        }
        names = {v["name"] for v in _topo_sort_views(views)}
        assert names == {"A", "B"}

    def test_dependent_view_comes_after_base(self):
        views = {
            "dbo.Dep":  {"name": "Dep",  "definition": "SELECT * FROM Base"},
            "dbo.Base": {"name": "Base", "definition": "SELECT 1"},
        }
        result = _topo_sort_views(views)
        names  = [v["name"] for v in result]
        assert names.index("Base") < names.index("Dep")

    def test_chain_dependency_order(self):
        views = {
            "dbo.C": {"name": "C", "definition": "SELECT * FROM B"},
            "dbo.B": {"name": "B", "definition": "SELECT * FROM A"},
            "dbo.A": {"name": "A", "definition": "SELECT 1"},
        }
        result = _topo_sort_views(views)
        names  = [v["name"] for v in result]
        assert names.index("A") < names.index("B") < names.index("C")

    def test_cyclic_dependency_does_not_hang(self):
        """Zyklische Abhängigkeiten werden akzeptiert ohne Endlosschleife."""
        views = {
            "dbo.X": {"name": "X", "definition": "SELECT * FROM Y"},
            "dbo.Y": {"name": "Y", "definition": "SELECT * FROM X"},
        }
        result = _topo_sort_views(views)
        assert len(result) == 2   # beide Views erscheinen trotzdem


# ════════════════════════════════════════════════════════════════════════════
#  generate_mysql_ddl
# ════════════════════════════════════════════════════════════════════════════
_MINIMAL_SCHEMA = {
    "tables": {
        "dbo.Users": {
            "schema": "dbo",
            "name":   "Users",
            "columns": [
                {
                    "name": "Id",   "pos": 1, "nullable": False, "type": "int",
                    "max_len": None, "precision": None, "scale": None,
                    "default": None, "identity": True,
                },
                {
                    "name": "Name", "pos": 2, "nullable": True, "type": "nvarchar",
                    "max_len": 100, "precision": None, "scale": None,
                    "default": None, "identity": False,
                },
            ],
            "pk": ["Id"],
            "fk": [],
        }
    },
    "views": {},
}

_FK_SCHEMA = {
    "tables": {
        "dbo.Users": {
            "schema": "dbo", "name": "Users",
            "columns": [{"name": "Id", "pos": 1, "nullable": False, "type": "int",
                         "max_len": None, "precision": None, "scale": None,
                         "default": None, "identity": True}],
            "pk": ["Id"], "fk": [],
        },
        "dbo.Orders": {
            "schema": "dbo", "name": "Orders",
            "columns": [
                {"name": "Id",     "pos": 1, "nullable": False, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": True},
                {"name": "UserId", "pos": 2, "nullable": False, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": False},
            ],
            "pk": ["Id"],
            "fk": [{"name": "FK_Orders_Users", "from_cols": ["UserId"],
                    "to_schema": "dbo", "to_table": "Users", "to_cols": ["Id"],
                    "on_delete": "CASCADE", "on_update": "NO_ACTION"}],
        },
    },
    "views": {},
}


class TestParseSimpleEqualityFilter:
    def test_parses_bracket_int_filter(self):
        assert _parse_simple_equality_filter("([IsDefault]=(1))") == ("IsDefault", "1")

    def test_parses_without_extra_parens(self):
        assert _parse_simple_equality_filter("[Active]=(0)") == ("Active", "0")

    def test_none_input_returns_none(self):
        assert _parse_simple_equality_filter(None) is None

    def test_unrecognized_pattern_returns_none(self):
        # Bereich-/IN-Filter etc. werden nicht unterstuetzt
        assert _parse_simple_equality_filter("([Status] IN (1,2))") is None


class TestRenderIndexDdl:
    def test_plain_index_unchanged(self):
        idx = {"name": "IX_Foo", "unique": False, "filter": None,
               "columns": [{"name": "Bar", "desc": False}]}
        lines = render_index_ddl("MyTable", idx)
        assert lines == ["CREATE INDEX `IX_Foo` ON `MyTable` (`Bar` ASC);"]

    def test_plain_unique_index_unchanged(self):
        idx = {"name": "UX_Foo", "unique": True, "filter": None,
               "columns": [{"name": "A", "desc": False}, {"name": "B", "desc": False}]}
        lines = render_index_ddl("MyTable", idx)
        assert lines == ["CREATE UNIQUE INDEX `UX_Foo` ON `MyTable` (`A` ASC, `B` ASC);"]

    def test_filtered_unique_index_emulated_via_generated_column(self):
        # Der reale Fall: TablePlatingRate.UX_TablePlatingRate_OneDefault
        # WHERE [IsDefault] = 1  ->  darf NICHT zu einem unkonditionalen
        # UNIQUE(PlatingId) werden (das war der Übersetzungsfehler).
        idx = {"name": "UX_TablePlatingRate_OneDefault", "unique": True,
               "filter": "([IsDefault]=(1))",
               "columns": [{"name": "PlatingId", "desc": False}]}
        lines = render_index_ddl("TablePlatingRate", idx)
        ddl = "\n".join(lines)
        assert "ADD COLUMN `_UX_TablePlatingRate_OneDefault_key`" in ddl
        assert "GENERATED ALWAYS AS (IF(`IsDefault` = 1, `PlatingId`, NULL)) VIRTUAL" in ddl
        assert "CREATE UNIQUE INDEX `UX_TablePlatingRate_OneDefault` ON `TablePlatingRate` (`_UX_TablePlatingRate_OneDefault_key`);" in ddl
        # Die ALTER-Anweisung fuer die generierte Spalte muss vor dem Index stehen
        assert ddl.index("ADD COLUMN") < ddl.index("CREATE UNIQUE INDEX")
        # Keine falsche "gleichwertig"-Behauptung mehr ohne echte Emulation
        assert "gleichwertiges Verhalten" not in ddl

    def test_filtered_index_with_multiple_columns_uses_concat(self):
        idx = {"name": "UX_Multi", "unique": True, "filter": "([Flag]=(1))",
               "columns": [{"name": "A", "desc": False}, {"name": "B", "desc": False}]}
        lines = render_index_ddl("T", idx)
        ddl = "\n".join(lines)
        assert "CONCAT_WS('|', `A`, `B`)" in ddl

    def test_unrecognized_filter_emits_warning_not_wrong_index(self):
        idx = {"name": "UX_Weird", "unique": True, "filter": "([Status] IN (1,2))",
               "columns": [{"name": "PlatingId", "desc": False}]}
        lines = render_index_ddl("T", idx)
        ddl = "\n".join(lines)
        assert "⚠" in ddl
        assert "manuell" in ddl.lower()
        # Es darf kein unkonditionaler UNIQUE-Index erzeugt werden
        assert "CREATE UNIQUE INDEX `UX_Weird` ON `T` (`PlatingId` ASC);" in ddl


class TestGenerateMysqlDdl:
    def test_contains_create_database(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "CREATE DATABASE IF NOT EXISTS `TestDB`" in ddl

    def test_contains_use_statement(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "USE `TestDB`" in ddl

    def test_foreign_key_checks_disabled_then_reenabled(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "SET FOREIGN_KEY_CHECKS = 0" in ddl
        assert "SET FOREIGN_KEY_CHECKS = 1" in ddl

    def test_table_created(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "CREATE TABLE `Users`" in ddl

    def test_identity_column_has_auto_increment(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "AUTO_INCREMENT" in ddl

    def test_primary_key_included(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "PRIMARY KEY (`Id`)" in ddl

    def test_nullable_column_has_no_not_null(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        # Name column is nullable → should NOT have NOT NULL
        lines = [l for l in ddl.splitlines() if "`Name`" in l]
        assert lines, "Name column not found in DDL"
        assert "NOT NULL" not in lines[0]

    def test_not_null_on_pk_column(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        lines = [l for l in ddl.splitlines() if "`Id`" in l and "AUTO_INCREMENT" in l]
        assert lines, "Id column not found in DDL"
        assert "NOT NULL" in lines[0]

    def test_foreign_key_generated(self):
        ddl = generate_mysql_ddl(_FK_SCHEMA, "TestDB")
        assert "FOREIGN KEY" in ddl
        assert "FK_Orders_Users" in ddl
        assert "REFERENCES `Users`" in ddl

    def test_foreign_key_on_delete_cascade_emitted(self):
        # TODO 1.1: Kaskadenregel aus der Quelle muss im MySQL-DDL landen.
        ddl = generate_mysql_ddl(_FK_SCHEMA, "TestDB")
        assert "REFERENCES `Users` (`Id`) ON DELETE CASCADE;" in ddl
        # NO_ACTION (on_update) erzeugt keine Klausel
        assert "ON UPDATE" not in ddl

    def test_foreign_key_set_default_warns_and_omits(self):
        import copy
        schema = copy.deepcopy(_FK_SCHEMA)
        schema["tables"]["dbo.Orders"]["fk"][0]["on_delete"] = "SET_DEFAULT"
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "SET_DEFAULT" in ddl and "-- ⚠" in ddl   # Warnkommentar vorhanden
        # Die eigentliche ALTER-Zeile darf keine ON DELETE-Klausel bekommen
        alter_line = next(l for l in ddl.splitlines()
                          if "ADD CONSTRAINT `FK_Orders_Users`" in l)
        assert "ON DELETE" not in alter_line

    def test_foreign_key_set_null_emitted(self):
        import copy
        schema = copy.deepcopy(_FK_SCHEMA)
        schema["tables"]["dbo.Orders"]["fk"][0]["on_delete"] = "SET_NULL"
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "ON DELETE SET NULL;" in ddl

    def test_computed_column_warning_in_ddl(self):
        # TODO 3.4: berechnete Spalte erzeugt Warnkommentar im DDL
        import copy
        schema = copy.deepcopy(_MINIMAL_SCHEMA)
        schema["tables"]["dbo.Users"]["columns"].append({
            "name": "Total", "pos": 3, "nullable": True, "type": "float",
            "max_len": None, "precision": None, "scale": None,
            "default": None, "identity": False, "computed": True,
        })
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "BERECHNETE Spalte" in ddl
        assert "`Total` DOUBLE" in ddl   # bleibt als normale Spalte erhalten

    def test_no_computed_warning_without_computed_columns(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "BERECHNETE Spalte" not in ddl

    def test_datetimeoffset_column_warns_about_lost_offset(self):
        # #63: datetimeoffset -> DATETIME verliert den Zeitzonen-Offset
        import copy
        schema = copy.deepcopy(_MINIMAL_SCHEMA)
        schema["tables"]["dbo.Users"]["columns"].append({
            "name": "EventAt", "pos": 3, "nullable": True, "type": "datetimeoffset",
            "max_len": None, "precision": None, "scale": 6,
            "default": None, "identity": False,
        })
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "Zeitzonen-Offset geht verloren" in ddl
        assert "`EventAt` DATETIME(6)" in ddl   # Spalte wird trotzdem angelegt

    def test_no_offset_warning_without_datetimeoffset(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "Zeitzonen-Offset" not in ddl

    def test_composite_foreign_key_single_constraint(self):
        # TODO 2.9: mehrspaltiger FK -> EIN ADD CONSTRAINT mit Spaltenlisten
        import copy
        schema = copy.deepcopy(_FK_SCHEMA)
        schema["tables"]["dbo.Orders"]["fk"] = [{
            "name": "FK_Orders_Composite", "from_cols": ["UserId", "TenantId"],
            "to_schema": "dbo", "to_table": "Users", "to_cols": ["Id", "Tenant"],
            "on_delete": "NO_ACTION", "on_update": "NO_ACTION",
        }]
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert ddl.count("ADD CONSTRAINT `FK_Orders_Composite`") == 1
        assert "FOREIGN KEY (`UserId`, `TenantId`)" in ddl
        assert "REFERENCES `Users` (`Id`, `Tenant`)" in ddl

    def test_view_included_after_tables(self):
        schema = dict(_MINIMAL_SCHEMA)
        schema["views"] = {
            "dbo.V": {"name": "V", "schema": "dbo",
                      "definition": "CREATE VIEW [dbo].[V] AS SELECT 1 AS n"}
        }
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "CREATE VIEW `V`" in ddl
        # View must come after the FOREIGN_KEY_CHECKS = 1 reset
        fk_pos   = ddl.index("SET FOREIGN_KEY_CHECKS = 1")
        view_pos = ddl.index("CREATE VIEW `V`")
        assert view_pos > fk_pos

    def test_utf8mb4_charset_on_tables(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        assert "utf8mb4" in ddl

    def test_drop_table_before_create(self):
        ddl = generate_mysql_ddl(_MINIMAL_SCHEMA, "TestDB")
        drop_pos   = ddl.index("DROP TABLE IF EXISTS `Users`")
        create_pos = ddl.index("CREATE TABLE `Users`")
        assert drop_pos < create_pos

    def test_view_audit_changes_excluded(self):
        # ViewAuditChanges basiert auf MSSQL-OPENJSON/FULL OUTER JOIN und wird
        # durch die Audit-Trigger (audit_triggers.py, P4) ersetzt.
        schema = dict(_MINIMAL_SCHEMA)
        schema["views"] = {
            "dbo.ViewAuditChanges": {
                "name": "ViewAuditChanges", "schema": "dbo",
                "definition": "CREATE VIEW [dbo].[ViewAuditChanges] AS SELECT 1 AS n",
            },
            "dbo.V": {"name": "V", "schema": "dbo",
                      "definition": "CREATE VIEW [dbo].[V] AS SELECT 1 AS n"},
        }
        ddl = generate_mysql_ddl(schema, "TestDB")
        assert "ViewAuditChanges" not in ddl
        assert "CREATE VIEW `V`" in ddl

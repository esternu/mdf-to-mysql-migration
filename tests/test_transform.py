"""
Tests für src/transform.py
Deckt ab: Typkonvertierung, Bezeichner-Mapping, Default-Konvertierung,
           View-SQL-Konvertierung, topologischer Sort und DDL-Generierung.
Keine externen Abhängigkeiten – keine Mocks erforderlich.
"""
import pytest
from transform import (
    convert_type,
    mssql_name,
    convert_default,
    convert_view_sql,
    generate_mysql_ddl,
    _topo_sort_views,
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

    def test_date(self):
        assert convert_type("date", None, None, None) == "DATE"

    # Sonstige
    def test_uniqueidentifier(self):
        assert convert_type("uniqueidentifier", None, None, None) == "CHAR(36)"

    def test_varbinary(self):
        assert convert_type("varbinary", None, None, None) == "LONGBLOB"

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

    def test_len_to_length(self):
        result, _ = convert_view_sql("SELECT LEN(col)")
        assert "LENGTH(" in result
        assert "LEN(" not in result

    # Entfernte Konstrukte
    def test_nolock_removed(self):
        result, _ = convert_view_sql("SELECT * FROM t WITH (NOLOCK)")
        assert "NOLOCK" not in result

    def test_top_removed(self):
        result, _ = convert_view_sql("SELECT TOP 10 col FROM t")
        assert "TOP" not in result

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
            "fk": [{"name": "FK_Orders_Users", "from_col": "UserId",
                    "to_schema": "dbo", "to_table": "Users", "to_col": "Id"}],
        },
    },
    "views": {},
}


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

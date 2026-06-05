"""
Tests für src/schema_diff.py:
  - diff_schemas: neue Tabellen, neue Spalten, Typ-Änderungen
  - generate_diff_ddl: korrekte ALTER TABLE / CREATE TABLE Ausgabe
  - format_diff_summary: lesbarer Text
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from schema_diff import diff_schemas, generate_diff_ddl, format_diff_summary


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def _src_table(name, columns, pk=None, fk=None, indexes=None):
    """Erzeugt einen Tabelleneintrag im MDF-Schema-Format (wie read_schema() liefert)."""
    return {
        "name":    name,
        "columns": columns,
        "pk":      pk or [],
        "fk":      fk or [],
        "indexes": indexes or [],
    }

def _src_col(name, sql_type="int", nullable=True, max_len=None, precision=None,
             scale=None, identity=False, default=None):
    return {
        "name": name, "type": sql_type, "nullable": nullable,
        "max_len": max_len, "precision": precision, "scale": scale,
        "identity": identity, "default": default,
    }

def _mysql_table(name, columns, indexes=None, fks=None):
    """Erzeugt einen Tabelleneintrag im MySQL-Schema-Format (wie read_mysql_schema() liefert)."""
    return {
        "columns": {c["name"]: {"type": c["type"], "nullable": c["nullable"],
                                "default": None, "auto_increment": False}
                    for c in columns},
        "pk":      [],
        "indexes": indexes or {},
        "fks":     fks or {},
    }

def _mysql_col(name, mysql_type, nullable=True):
    return {"name": name, "type": mysql_type, "nullable": nullable}


# ── Tests: diff_schemas ───────────────────────────────────────────────────────

def test_new_table_detected():
    src = {"tables": {
        "t1": _src_table("TableNew", [_src_col("id")])
    }}
    tgt = {"tables": {}}
    diff = diff_schemas(src, tgt)
    assert len(diff["new_tables"]) == 1
    assert diff["new_tables"][0]["name"] == "TableNew"
    assert diff["altered_tables"] == {}


def test_no_changes():
    col = _src_col("id", "int")
    src = {"tables": {"t1": _src_table("MyTable", [col])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [_mysql_col("id", "INT")])}}
    diff = diff_schemas(src, tgt)
    assert diff["new_tables"] == []
    assert diff["altered_tables"] == {}
    assert diff["warnings"] == []


def test_new_column_detected():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("id", "int"),
        _src_col("new_col", "nvarchar", max_len=100),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("id", "INT"),
    ])}}
    diff = diff_schemas(src, tgt)
    assert "MyTable" in diff["altered_tables"]
    new_cols = diff["altered_tables"]["MyTable"]["new_columns"]
    assert len(new_cols) == 1
    assert new_cols[0]["name"] == "new_col"


def test_type_change_generates_warning():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("amount", "decimal", precision=19, scale=4),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("amount", "INT"),
    ])}}
    diff = diff_schemas(src, tgt)
    assert "MyTable" in diff["altered_tables"]
    mods = diff["altered_tables"]["MyTable"]["modified_columns"]
    assert len(mods) == 1
    assert mods[0][0] == "amount"
    assert any("amount" in w for w in diff["warnings"])


def test_removed_table_only_warns():
    src = {"tables": {}}
    tgt = {"tables": {"oldie": _mysql_table("oldie", [_mysql_col("id", "INT")])}}
    diff = diff_schemas(src, tgt)
    assert "oldie" in diff["removed_tables"]
    assert any("oldie" in w for w in diff["warnings"])
    # Kein DROP-Statement – nur Warnung


def test_removed_column_only_warns():
    src = {"tables": {"t1": _src_table("MyTable", [_src_col("id", "int")])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("id", "INT"),
        _mysql_col("old_col", "VARCHAR(50)"),
    ])}}
    diff = diff_schemas(src, tgt)
    assert "MyTable" in diff["removed_columns"]
    assert any("old_col" in w for w in diff["warnings"])


# ── Tests: generate_diff_ddl ──────────────────────────────────────────────────

def test_generate_create_table():
    src = {"tables": {"t1": _src_table("NewTable", [
        _src_col("id", "int", nullable=False, identity=True),
        _src_col("name", "nvarchar", nullable=False, max_len=100),
    ], pk=["id"])}}
    tgt = {"tables": {}}
    diff = diff_schemas(src, tgt)
    ddl, warns = generate_diff_ddl(diff, src, "testdb")
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "`NewTable`" in ddl
    assert "AUTO_INCREMENT" in ddl
    assert "NOT NULL" in ddl


def test_generate_add_column():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("id", "int"),
        _src_col("extra", "nvarchar", max_len=50),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [_mysql_col("id", "INT")])}}
    diff = diff_schemas(src, tgt)
    ddl, warns = generate_diff_ddl(diff, src, "testdb")
    assert "ALTER TABLE" in ddl
    assert "ADD COLUMN" in ddl
    assert "`extra`" in ddl


def test_generate_modify_column_has_warning_comment():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("amount", "decimal", precision=19, scale=4),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [_mysql_col("amount", "INT")])}}
    diff = diff_schemas(src, tgt)
    ddl, warns = generate_diff_ddl(diff, src, "testdb")
    assert "MODIFY COLUMN" in ddl
    assert "-- ⚠" in ddl          # Warnungskommentar im DDL
    assert len(warns) > 0          # Warnliste befüllt


def test_no_changes_produces_minimal_ddl():
    src = {"tables": {"t1": _src_table("MyTable", [_src_col("id", "int")])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [_mysql_col("id", "INT")])}}
    diff = diff_schemas(src, tgt)
    ddl, warns = generate_diff_ddl(diff, src, "testdb")
    assert "CREATE TABLE" not in ddl
    assert "ALTER TABLE" not in ddl
    assert warns == []


# ── Tests: format_diff_summary ────────────────────────────────────────────────

def test_format_summary_no_changes():
    diff = {
        "new_tables": [], "altered_tables": {},
        "removed_tables": [], "removed_columns": {}, "warnings": [],
    }
    summary = format_diff_summary(diff)
    assert "aktuell" in summary.lower()


def test_format_summary_new_table():
    diff = {
        "new_tables": [{"name": "FreshTable"}],
        "altered_tables": {}, "removed_tables": [],
        "removed_columns": {}, "warnings": [],
    }
    summary = format_diff_summary(diff)
    assert "FreshTable" in summary

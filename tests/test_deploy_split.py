"""
Tests für deploy._split_statements.
Stellt sicher dass Semikolons in -- Kommentaren und String-Literalen
nicht als Statement-Trenner behandelt werden.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from deploy import _split_statements


def test_simple_split():
    ddl = "CREATE TABLE `a` (id INT); CREATE TABLE `b` (id INT);"
    result = _split_statements(ddl)
    assert len(result) == 2
    assert "CREATE TABLE `a`" in result[0]
    assert "CREATE TABLE `b`" in result[1]


def test_semicolon_in_line_comment_not_split():
    """Semikolon in -- Kommentar darf Statement nicht teilen."""
    ddl = (
        "CREATE VIEW `ViewRoundResults` AS\n"
        "-- decimal(18,<col>) waere ungueltig;\n"
        "-- (Skalierung muss eine Konstante sein).\n"
        "SELECT 1 AS x;"
    )
    result = _split_statements(ddl)
    assert len(result) == 1, f"Erwartet 1 Statement, got {len(result)}: {result}"
    assert "SELECT 1 AS x" in result[0]
    assert "waere ungueltig" in result[0]


def test_semicolon_in_string_literal_not_split():
    """Semikolon innerhalb eines String-Literals darf nicht trennen."""
    ddl = "INSERT INTO t VALUES ('hello; world'); SELECT 1;"
    result = _split_statements(ddl)
    assert len(result) == 2
    assert "hello; world" in result[0]


def test_block_comment_with_semicolon():
    """Semikolon in /* */ Kommentar darf nicht trennen."""
    ddl = "/* init; setup */ CREATE TABLE t (id INT);"
    result = _split_statements(ddl)
    assert len(result) == 1


def test_multiple_views_correct_count():
    """Mehrere Views und Tabellen werden korrekt aufgeteilt."""
    ddl = (
        "CREATE TABLE `t1` (id INT);\n"
        "\n"
        "CREATE VIEW `v1` AS\n"
        "-- Diese View ist komplex; sie hat Kommentare\n"
        "SELECT id FROM `t1`;\n"
        "\n"
        "CREATE VIEW `v2` AS SELECT 1;\n"
    )
    result = _split_statements(ddl)
    assert len(result) == 3, f"Erwartet 3, got {len(result)}: {result}"


def test_no_trailing_empty_statements():
    """Leere Einträge nach dem letzten ; werden nicht zurückgegeben."""
    ddl = "SELECT 1; SELECT 2;   \n  "
    result = _split_statements(ddl)
    assert len(result) == 2


def test_escaped_quote_in_string():
    """Escaped Quote '' innerhalb String-Literals wird korrekt behandelt."""
    ddl = "INSERT INTO t VALUES ('it''s fine; really'); SELECT 1;"
    result = _split_statements(ddl)
    assert len(result) == 2
    assert "it''s fine; really" in result[0]

"""
Tests für src/schema_diff.py:
  - diff_schemas: neue Tabellen, neue Spalten, Typ-Änderungen
  - generate_diff_ddl: korrekte ALTER TABLE / CREATE TABLE Ausgabe
  - format_diff_summary: lesbarer Text
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from schema_diff import diff_schemas, generate_diff_ddl, format_diff_summary, detect_rename_candidates


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
             scale=None, identity=False, default=None, pos=None):
    return {
        "name": name, "type": sql_type, "nullable": nullable,
        "max_len": max_len, "precision": precision, "scale": scale,
        "identity": identity, "default": default, "pos": pos,
    }

def _mysql_table(name, columns, indexes=None, fks=None):
    """Erzeugt einen Tabelleneintrag im MySQL-Schema-Format (wie read_mysql_schema() liefert)."""
    return {
        "columns": {c["name"]: {"type": c["type"], "nullable": c["nullable"],
                                "default": None, "auto_increment": False,
                                "pos": c.get("pos")}
                    for c in columns},
        "pk":      [],
        "indexes": indexes or {},
        "fks":     fks or {},
    }

def _mysql_col(name, mysql_type, nullable=True, pos=None):
    return {"name": name, "type": mysql_type, "nullable": nullable, "pos": pos}


def _src_view(name, tsql):
    return {"schema": "dbo", "name": name, "definition": tsql}


# ── Tests: View-Sync im Schema-Diff (#67) ──────────────────────────────────────

def _views_src(name, tsql):
    return {"tables": {}, "views": {f"dbo.{name}": _src_view(name, tsql)}}


def _views_tgt(name, columns):
    return {"tables": {}, "views": {name: {"columns": columns}}}


def test_view_column_rename_detected():
    # ViewSurfaceArticle-Fall: gleiche Logik, Spalte umbenannt -> neu erstellen
    src = _views_src("V", "SELECT a AS [Bath Surface], b AS [Total Surface] FROM t")
    tgt = _views_tgt("V", ["Article Surface", "Total Surface"])
    diff = diff_schemas(src, tgt)
    assert [v["name"] for v in diff["changed_views"]] == ["V"]


def test_view_identical_columns_no_change():
    src = _views_src("V", "SELECT a AS [Col1], b AS [Col2] FROM t")
    tgt = _views_tgt("V", ["Col1", "Col2"])
    diff = diff_schemas(src, tgt)
    assert diff["changed_views"] == []


def test_new_view_not_in_mysql_is_changed():
    src = _views_src("V", "SELECT a AS [Col1] FROM t")
    tgt = {"tables": {}, "views": {}}
    diff = diff_schemas(src, tgt)
    assert [v["name"] for v in diff["changed_views"]] == ["V"]


def test_excluded_view_not_synced():
    src = _views_src("ViewAuditChanges", "SELECT a AS [Col1] FROM t")
    tgt = {"tables": {}, "views": {}}
    diff = diff_schemas(src, tgt)
    assert diff["changed_views"] == []


def test_generate_diff_ddl_recreates_changed_view():
    src = _views_src("V", "CREATE VIEW [dbo].[V] AS SELECT a AS [Bath Surface] FROM t")
    tgt = _views_tgt("V", ["Article Surface"])
    diff = diff_schemas(src, tgt)
    ddl, _ = generate_diff_ddl(diff, src, "testdb")
    assert "DROP VIEW IF EXISTS `V`;" in ddl
    assert "CREATE VIEW `V` AS" in ddl
    assert "`Bath Surface`" in ddl


def test_summary_lists_changed_views():
    src = _views_src("V", "SELECT a AS [X] FROM t")
    tgt = _views_tgt("V", ["Y"])
    diff = diff_schemas(src, tgt)
    summary = format_diff_summary(diff)
    assert "Geänderte/neue Views" in summary and "~ V" in summary


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


def test_tinyint_display_width_is_not_a_real_type_change():
    # TINYINT(3) UNSIGNED (alte MySQL-Spalte) und TINYINT UNSIGNED (vom
    # Generator erzeugt) sind funktional identisch - die Display-Width
    # wird seit MySQL 8/MariaDB ignoriert. Ohne Normalisierung wurde das
    # bei jedem Schema-Diff-Lauf faelschlich als Typ-Aenderung gemeldet.
    src = {"tables": {"t1": _src_table("TableSolution", [
        _src_col("Suitable Resins", "tinyint"),
    ])}}
    tgt = {"tables": {"tablesolution": _mysql_table("tablesolution", [
        _mysql_col("Suitable Resins", "TINYINT(3) UNSIGNED"),
    ])}}
    diff = diff_schemas(src, tgt)
    assert diff["altered_tables"] == {}
    assert diff["warnings"] == []


def test_tinyint_1_display_width_still_kept_distinct_from_plain_tinyint():
    # TINYINT(1) entspricht BIT/bool und darf nicht mit TINYINT (ohne
    # Bool-Bedeutung) gleichgesetzt werden.
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("flag", "bit"),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("flag", "TINYINT(3) UNSIGNED"),
    ])}}
    diff = diff_schemas(src, tgt)
    mods = diff["altered_tables"]["MyTable"]["modified_columns"]
    assert mods[0] == ("flag", "TINYINT UNSIGNED", "TINYINT(1)")


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


def test_removed_column_entry_includes_name():
    # format_diff_summary() braucht "name" im removed_columns-Eintrag,
    # sonst landet der rohe dict-repr im Log statt des Spaltennamens.
    src = {"tables": {"t1": _src_table("MyTable", [_src_col("id", "int")])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("id", "INT"),
        _mysql_col("old_col", "VARCHAR(50)"),
    ])}}
    diff = diff_schemas(src, tgt)
    summary = format_diff_summary(diff)
    assert "old_col" in summary
    assert "{'type'" not in summary


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


def test_new_table_in_diff_includes_indexes_and_fks():
    # TODO 2.11: neue Tabelle im Diff bekam nur Spalten+PK - Indexe und
    # FKs fehlten (so entstand TablePlatingRate ohne UNIQUE-Constraints).
    src = {"tables": {"t1": _src_table(
        "TablePlatingRate",
        [_src_col("Id", "int", nullable=False, identity=True),
         _src_col("PlatingId", "int", nullable=False),
         _src_col("Current Density", "float", nullable=False)],
        pk=["Id"],
        fk=[{"name": "FK_TablePlatingRate_TablePlating",
             "from_cols": ["PlatingId"], "to_cols": ["Id"],
             "to_schema": "dbo", "to_table": "TablePlating",
             "on_delete": "CASCADE", "on_update": "NO_ACTION"}],
        indexes=[{"name": "UX_TablePlatingRate_Plating_CD", "unique": True,
                  "filter": None,
                  "columns": [{"name": "PlatingId", "desc": False},
                              {"name": "Current Density", "desc": False}]}],
    )}}
    tgt  = {"tables": {}}
    diff = diff_schemas(src, tgt)
    ddl, _ = generate_diff_ddl(diff, src, "testdb")
    assert "CREATE TABLE IF NOT EXISTS `TablePlatingRate`" in ddl
    assert "CREATE UNIQUE INDEX `UX_TablePlatingRate_Plating_CD`" in ddl
    assert "ADD CONSTRAINT `FK_TablePlatingRate_TablePlating`" in ddl
    assert "ON DELETE CASCADE;" in ddl


def test_generated_column_not_reported_as_removed():
    # TODO 2.10: _UX_*_key-Hilfsspalten (GENERATED) sind tool-eigene
    # Artefakte - keine Dauerwarnung "existiert nicht mehr in MDF".
    src = {"tables": {"t1": _src_table("MyTable", [_src_col("Id", "int")])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("Id", "INT"),
    ])}}
    tgt["tables"]["mytable"]["columns"]["_UX_MyTable_OneDefault_key"] = {
        "type": "VARCHAR(255)", "nullable": True, "default": None,
        "auto_increment": False, "generated": True, "pos": 2,
    }
    diff = diff_schemas(src, tgt)
    assert diff["removed_columns"] == {}
    assert diff["warnings"] == []


def test_generated_column_not_a_rename_candidate():
    # Die generierte VARCHAR(255)-Hilfsspalte darf nicht als Umbenennungs-
    # Kandidat fuer eine neue echte VARCHAR-Spalte vorgeschlagen werden
    # (der bestaetigte Rename wuerde sie DROPpen!).
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("Id", "int", pos=1),
        _src_col("NewName", "nvarchar", max_len=255, pos=2),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("Id", "INT", pos=1),
    ])}}
    tgt["tables"]["mytable"]["columns"]["_UX_MyTable_OneDefault_key"] = {
        "type": "VARCHAR(255)", "nullable": True, "default": None,
        "auto_increment": False, "generated": True, "pos": 2,
    }
    diff = diff_schemas(src, tgt)
    candidates = detect_rename_candidates(diff, src, tgt)
    assert candidates == {}


def test_datetime_fsp_zero_equals_plain_datetime():
    # TODO 2.6: datetime2(0) -> DATETIME; MySQL meldet "datetime" -> kein Diff.
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("CreatedAt", "datetime2", scale=0),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("CreatedAt", "DATETIME"),
    ])}}
    diff = diff_schemas(src, tgt)
    assert diff["altered_tables"] == {}


def test_datetime_fsp_mismatch_detected():
    # Bestehende DATETIME(6)-Spalte vs. Quelle datetime2(0) -> Typkorrektur.
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("CreatedAt", "datetime2", scale=0),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("CreatedAt", "DATETIME(6)"),
    ])}}
    diff = diff_schemas(src, tgt)
    mods = diff["altered_tables"]["MyTable"]["modified_columns"]
    assert mods[0] == ("CreatedAt", "DATETIME(6)", "DATETIME")


# ── Tests: FK-Regeln (ON DELETE/UPDATE) ───────────────────────────────────────

def _fk_src_table():
    return _src_table("Orders", [_src_col("Id", "int"), _src_col("UserId", "int")],
                      pk=["Id"],
                      fk=[{"name": "FK_Orders_Users", "from_cols": ["UserId"],
                           "to_schema": "dbo", "to_table": "Users", "to_cols": ["Id"],
                           "on_delete": "CASCADE", "on_update": "NO_ACTION"}])


def _fk_mysql_table(on_delete="RESTRICT"):
    t = _mysql_table("orders", [_mysql_col("Id", "INT"), _mysql_col("UserId", "INT")])
    t["fks"]["FK_Orders_Users"] = {
        "from_cols": ["UserId"], "to_table": "Users", "to_cols": ["Id"],
        "on_delete": on_delete, "on_update": "RESTRICT",
    }
    return t


def test_fk_rule_mismatch_detected_and_fixed():
    # TODO 1.1: Live-DB hat RESTRICT, Quelle will CASCADE -> DROP + ADD im Diff.
    src  = {"tables": {"t1": _fk_src_table()}}
    tgt  = {"tables": {"orders": _fk_mysql_table("RESTRICT")}}
    diff = diff_schemas(src, tgt)
    assert len(diff["altered_tables"]["Orders"]["modified_fks"]) == 1
    assert any("FK-Regel weicht ab" in w for w in diff["warnings"])

    ddl, _ = generate_diff_ddl(diff, src, "testdb")
    assert "DROP FOREIGN KEY `FK_Orders_Users`" in ddl
    assert "ON DELETE CASCADE;" in ddl


def test_fk_rule_match_produces_no_change():
    # NO_ACTION (MSSQL) und RESTRICT (MySQL) sind gleichwertig -> kein Diff.
    src = {"tables": {"t1": _fk_src_table()}}
    src["tables"]["t1"]["fk"][0]["on_delete"] = "NO_ACTION"
    tgt  = {"tables": {"orders": _fk_mysql_table("RESTRICT")}}
    diff = diff_schemas(src, tgt)
    assert diff["altered_tables"] == {}


def test_new_fk_in_diff_carries_cascade():
    src = {"tables": {"t1": _fk_src_table()}}
    tgt = {"tables": {"orders": _mysql_table("orders", [
        _mysql_col("Id", "INT"), _mysql_col("UserId", "INT")])}}
    diff = diff_schemas(src, tgt)
    ddl, _ = generate_diff_ddl(diff, src, "testdb")
    assert "ADD CONSTRAINT `FK_Orders_Users`" in ddl
    assert "ON DELETE CASCADE;" in ddl


# ── Tests: detect_rename_candidates ───────────────────────────────────────────

def test_rename_candidate_detected_when_unambiguous():
    src = {"tables": {"t1": _src_table("TableSolution", [
        _src_col("Id", "int", pos=1),
        _src_col("Bath Surface", "float", pos=2),
    ])}}
    tgt = {"tables": {"tablesolution": _mysql_table("tablesolution", [
        _mysql_col("Id", "INT", pos=1),
        _mysql_col("surface", "DOUBLE", pos=2),
    ])}}
    diff = diff_schemas(src, tgt)
    candidates = detect_rename_candidates(diff, src, tgt)
    assert candidates["TableSolution"] == [("surface", "Bath Surface", "DOUBLE")]


def test_no_rename_candidate_when_types_differ():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("Id", "int", pos=1),
        _src_col("new_col", "nvarchar", max_len=50, pos=2),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [
        _mysql_col("Id", "INT", pos=1),
        _mysql_col("old_col", "DOUBLE", pos=2),
    ])}}
    diff = diff_schemas(src, tgt)
    candidates = detect_rename_candidates(diff, src, tgt)
    assert candidates == {}


def test_ambiguous_candidates_resolved_by_neighbors():
    # Zwei DOUBLE-Spalten gleichzeitig entfernt/hinzugefuegt - nur das Paar
    # mit passenden Nachbarn (gleiche Vorgaenger-/Nachfolger-Spalte) gilt.
    src = {"tables": {"t1": _src_table("TableX", [
        _src_col("Id", "int", pos=1),
        _src_col("Weight New", "float", pos=2),   # Nachbarn: Id / Marker
        _src_col("Marker", "nvarchar", max_len=10, pos=3),
        _src_col("Other New", "float", pos=4),     # Nachbarn: Marker / Tail
        _src_col("Tail", "int", pos=5),
    ])}}
    tgt = {"tables": {"tablex": _mysql_table("tablex", [
        _mysql_col("Id", "INT", pos=1),
        _mysql_col("Weight Old", "DOUBLE", pos=2),    # Nachbarn: Id / Marker (match)
        _mysql_col("Marker", "VARCHAR(10)", pos=3),
        _mysql_col("Unrelated Old", "DOUBLE", pos=4), # Nachbarn: Marker / DifferentTail
        _mysql_col("DifferentTail", "INT", pos=5),
    ])}}
    diff = diff_schemas(src, tgt)
    candidates = detect_rename_candidates(diff, src, tgt)
    # DOUBLE ist mehrdeutig -> nur das Nachbar-passende Paar zaehlt;
    # INT (DifferentTail/Tail) ist je Typ eindeutig -> wird ebenfalls erkannt.
    assert set(candidates["TableX"]) == {
        ("Weight Old", "Weight New", "DOUBLE"),
        ("DifferentTail", "Tail", "INT"),
    }


def test_ambiguous_candidates_dropped_without_neighbor_match():
    src = {"tables": {"t1": _src_table("TableY", [
        _src_col("New A", "float", pos=1),
        _src_col("New B", "float", pos=2),
    ])}}
    tgt = {"tables": {"tabley": _mysql_table("tabley", [
        _mysql_col("Old A", "DOUBLE", pos=1),
        _mysql_col("Old B", "DOUBLE", pos=2),
    ])}}
    diff = diff_schemas(src, tgt)
    candidates = detect_rename_candidates(diff, src, tgt)
    assert candidates == {}


def test_generate_diff_ddl_emits_update_for_confirmed_rename():
    src = {"tables": {"t1": _src_table("TableSolution", [
        _src_col("Id", "int", pos=1),
        _src_col("Bath Surface", "float", pos=2),
    ])}}
    tgt = {"tables": {"tablesolution": _mysql_table("tablesolution", [
        _mysql_col("Id", "INT", pos=1),
        _mysql_col("surface", "DOUBLE", pos=2),
    ])}}
    diff = diff_schemas(src, tgt)
    rename_pairs = {"TableSolution": [("surface", "Bath Surface")]}
    ddl, _ = generate_diff_ddl(diff, src, "testdb", rename_pairs)
    assert "ADD COLUMN `Bath Surface`" in ddl
    assert "UPDATE `TableSolution` SET `Bath Surface` = `surface`;" in ddl
    assert "DROP COLUMN `surface`;" in ddl
    # Reihenfolge: erst Spalte anlegen, dann Werte kopieren, dann alte loeschen
    assert (
        ddl.index("ADD COLUMN `Bath Surface`")
        < ddl.index("UPDATE `TableSolution`")
        < ddl.index("DROP COLUMN `surface`")
    )
    # Umbenannte Spalte taucht nicht mehr in der "nicht geloescht"-Warnung auf
    assert "Spalte:  TableSolution.surface" not in ddl


def test_generate_diff_ddl_unrenamed_removed_column_still_warned():
    # Eine entfernte Spalte, die NICHT als Umbenennung bestaetigt wurde,
    # bleibt weiterhin in der Warnung am Ende.
    src = {"tables": {"t1": _src_table("TableSolution", [
        _src_col("Id", "int", pos=1),
        _src_col("Bath Surface", "float", pos=2),
    ])}}
    tgt = {"tables": {"tablesolution": _mysql_table("tablesolution", [
        _mysql_col("Id", "INT", pos=1),
        _mysql_col("surface", "DOUBLE", pos=2),
        _mysql_col("Unrelated", "DOUBLE", pos=3),
    ])}}
    diff = diff_schemas(src, tgt)
    rename_pairs = {"TableSolution": [("surface", "Bath Surface")]}
    ddl, _ = generate_diff_ddl(diff, src, "testdb", rename_pairs)
    assert "DROP COLUMN `surface`;" in ddl
    assert "Spalte:  TableSolution.Unrelated" in ddl


def test_generate_diff_ddl_without_rename_pairs_unchanged():
    src = {"tables": {"t1": _src_table("MyTable", [
        _src_col("id", "int"),
        _src_col("extra", "nvarchar", max_len=50),
    ])}}
    tgt = {"tables": {"mytable": _mysql_table("mytable", [_mysql_col("id", "INT")])}}
    diff = diff_schemas(src, tgt)
    ddl, _ = generate_diff_ddl(diff, src, "testdb")
    assert "UPDATE" not in ddl


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

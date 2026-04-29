"""
Tests für src/migrate_data.py

Abgedeckte Funktionen:
  - get_table_list   (TestGetTableList)
  - read_table_data  (TestReadTableData)
  - migrate_table    (TestMigrateTable)
  - migrate_all      (TestMigrateAll)

Alle externen Abhängigkeiten (pyodbc-Session, mysql.connector) werden
vollständig durch Mocks ersetzt – kein laufender Datenbankserver nötig.
"""
import pytest
from migrate_data import (
    get_table_list,
    read_table_data,
    migrate_table,
    migrate_all,
)


# ════════════════════════════════════════════════════════════════════════════
#  Hilfsmittel / Fixtures
# ════════════════════════════════════════════════════════════════════════════

def _noop(msg: str) -> None:
    """Leerer Log-Callback."""
    pass


class _MockCursor:
    """Simpler Cursor-Stub der aufeinanderfolgende fetchall()-Ergebnisse liefert."""

    def __init__(self, fetchall_results=None, side_effect=None):
        """
        fetchall_results : list of lists
            Jedes innere Element wird von einem fetchall()-Aufruf zurückgegeben.
        side_effect : Exception | None
            Falls gesetzt, wird diese bei execute() geworfen.
        """
        self._results   = iter(fetchall_results or [[]])
        self._effect    = side_effect
        self.executed   = []
        self.many_calls = []   # args von executemany
        self.closed     = False

    def execute(self, sql, *args):
        if self._effect:
            raise self._effect
        self.executed.append((sql,) + args)

    def executemany(self, sql, batch):
        if self._effect:
            raise self._effect
        self.many_calls.append((sql, list(batch)))

    def fetchall(self):
        return list(next(self._results, []))

    def close(self):
        self.closed = True


class _MockSession:
    """Ersetzt MdfSession – liefert einen konfigurierbaren _MockCursor."""

    def __init__(self, fetchall_results=None, side_effect=None):
        self._cur = _MockCursor(fetchall_results, side_effect)

    def cursor(self):
        return self._cur


class _MockMySQLConn:
    """Minimaler mysql.connector.connection-Mock."""

    def __init__(self, cursor_side_effect=None):
        self._cur           = _MockCursor(side_effect=cursor_side_effect)
        self.committed      = 0
        self.rolled_back    = 0
        self.closed         = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True


# ════════════════════════════════════════════════════════════════════════════
#  get_table_list
# ════════════════════════════════════════════════════════════════════════════
class TestGetTableList:

    def test_returns_list_of_tuples(self):
        rows    = [("dbo", "TableArticle"), ("dbo", "TableCost")]
        session = _MockSession(fetchall_results=[rows])
        result  = get_table_list(session)
        assert result == [("dbo", "TableArticle"), ("dbo", "TableCost")]

    def test_empty_database_returns_empty_list(self):
        session = _MockSession(fetchall_results=[[]])
        assert get_table_list(session) == []

    def test_single_table(self):
        session = _MockSession(fetchall_results=[[("dbo", "TableUnits")]])
        result  = get_table_list(session)
        assert len(result) == 1
        assert result[0] == ("dbo", "TableUnits")

    def test_result_is_list_of_tuples_not_rows(self):
        """Rückgabe muss eine echte Python-Liste aus Tupeln sein."""
        session = _MockSession(fetchall_results=[[("dbo", "T1"), ("dbo", "T2")]])
        result  = get_table_list(session)
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, tuple)

    def test_schema_preserved(self):
        rows    = [("myschema", "MyTable")]
        session = _MockSession(fetchall_results=[rows])
        schema, name = get_table_list(session)[0]
        assert schema == "myschema"
        assert name   == "MyTable"

    def test_multiple_schemas(self):
        rows    = [("dbo", "T1"), ("ext", "T2"), ("dbo", "T3")]
        session = _MockSession(fetchall_results=[rows])
        result  = get_table_list(session)
        schemas = [r[0] for r in result]
        assert "dbo" in schemas and "ext" in schemas


# ════════════════════════════════════════════════════════════════════════════
#  read_table_data
# ════════════════════════════════════════════════════════════════════════════
class TestReadTableData:

    def test_returns_columns_and_rows(self):
        col_rows  = [("Index",), ("Name",), ("Product",)]
        data_rows = [(1, "Bangle 1", "Silver"), (2, "Bangle 2", "Gold")]
        session   = _MockSession(fetchall_results=[col_rows, data_rows])
        cols, rows = read_table_data(session, "dbo", "TableArticle")
        assert cols  == ["Index", "Name", "Product"]
        assert rows  == [(1, "Bangle 1", "Silver"), (2, "Bangle 2", "Gold")]

    def test_empty_table_returns_empty_rows(self):
        col_rows = [("Id",), ("Val",)]
        session  = _MockSession(fetchall_results=[col_rows, []])
        cols, rows = read_table_data(session, "dbo", "EmptyTable")
        assert cols  == ["Id", "Val"]
        assert rows  == []

    def test_column_count_matches(self):
        col_rows  = [("A",), ("B",), ("C",), ("D",)]
        data_rows = [(1, 2, 3, 4)]
        session   = _MockSession(fetchall_results=[col_rows, data_rows])
        cols, rows = read_table_data(session, "dbo", "T")
        assert len(cols) == 4
        assert len(rows[0]) == 4

    def test_single_column_table(self):
        session = _MockSession(fetchall_results=[[("ID",)], [(42,), (43,)]])
        cols, rows = read_table_data(session, "dbo", "T")
        assert cols == ["ID"]
        assert rows == [(42,), (43,)]

    def test_none_values_preserved(self):
        """NULL-Werte müssen als Python-None ankommen."""
        session = _MockSession(
            fetchall_results=[[("A",), ("B",)], [(1, None), (None, 2)]]
        )
        _, rows = read_table_data(session, "dbo", "T")
        assert rows[0][1] is None
        assert rows[1][0] is None


# ════════════════════════════════════════════════════════════════════════════
#  migrate_table
# ════════════════════════════════════════════════════════════════════════════
class TestMigrateTable:

    def test_returns_row_count(self):
        conn  = _MockMySQLConn()
        count = migrate_table(conn, "TableArticle", ["Id", "Name"], [(1, "A"), (2, "B")], _noop)
        assert count == 2

    def test_empty_rows_returns_zero(self):
        conn  = _MockMySQLConn()
        count = migrate_table(conn, "T", ["Id"], [], _noop)
        assert count == 0

    def test_empty_rows_skipped_message_logged(self):
        conn   = _MockMySQLConn()
        logged = []
        migrate_table(conn, "MyTable", ["Id"], [], logged.append)
        assert any("übersprungen" in l or "skipped" in l.lower() for l in logged)

    def test_truncate_called_before_insert(self):
        conn  = _MockMySQLConn()
        migrate_table(conn, "T", ["Id"], [(1,)], _noop)
        first_sql = conn._cur.executed[0][0].upper()
        assert "TRUNCATE" in first_sql

    def test_executemany_called_with_correct_table(self):
        conn  = _MockMySQLConn()
        migrate_table(conn, "TableCost", ["A", "B"], [(1, 2), (3, 4)], _noop)
        assert len(conn._cur.many_calls) == 1
        sql, batch = conn._cur.many_calls[0]
        assert "TableCost" in sql
        assert batch == [(1, 2), (3, 4)]

    def test_commit_called_after_insert(self):
        conn  = _MockMySQLConn()
        migrate_table(conn, "T", ["Id"], [(1,)], _noop)
        assert conn.committed >= 1

    def test_success_logged(self):
        conn   = _MockMySQLConn()
        logged = []
        migrate_table(conn, "TablePlating", ["Id"], [(1,), (2,), (3,)], logged.append)
        combined = " ".join(logged)
        assert "3" in combined and "TablePlating" in combined

    def test_memoryview_converted_to_bytes(self):
        """memoryview-Werte (BLOB) müssen transparent zu bytes konvertiert werden."""
        conn  = _MockMySQLConn()
        data  = memoryview(b"\x00\x01\x02")
        migrate_table(conn, "T", ["Img"], [(data,)], _noop)
        _, batch = conn._cur.many_calls[0]
        assert isinstance(batch[0][0], (bytes, memoryview))

    def test_column_list_in_insert_sql(self):
        conn  = _MockMySQLConn()
        migrate_table(conn, "T", ["ColA", "ColB"], [(1, 2)], _noop)
        sql, _ = conn._cur.many_calls[0]
        assert "`ColA`" in sql
        assert "`ColB`" in sql

    def test_raises_on_mysql_error(self):
        """Fehler vom Cursor werden nach oben weitergeleitet."""
        import mysql.connector
        conn          = _MockMySQLConn(cursor_side_effect=mysql.connector.Error("fail"))
        with pytest.raises(Exception):
            migrate_table(conn, "T", ["Id"], [(1,)], _noop)


# ════════════════════════════════════════════════════════════════════════════
#  migrate_all
# ════════════════════════════════════════════════════════════════════════════
class TestMigrateAll:

    def _make_session(self, tables_data: dict):
        """
        tables_data : {table_name: (columns_rows, data_rows)}
        Beispiel:
            {"TableArticle": ([("Id",)], [(1,), (2,)])}
        """
        # get_table_list wird nicht über _MockSession gerufen (kommt extern),
        # aber read_table_data ruft cursor() → fetchall() zweimal pro Tabelle.
        # Wir bauen die fetchall-Sequenz für alle Tabellen auf.
        all_results = []
        for col_rows, data_rows in tables_data.values():
            all_results.append(col_rows)
            all_results.append(data_rows)
        return _MockSession(fetchall_results=all_results)

    def test_returns_correct_total_rows(self):
        tables  = [("dbo", "T1"), ("dbo", "T2")]
        session = self._make_session({
            "T1": ([("Id",)], [(1,), (2,)]),
            "T2": ([("Id",)], [(3,)]),
        })
        conn   = _MockMySQLConn()
        result = migrate_all(session, conn, tables, _noop)
        assert result["total_rows"] == 3

    def test_empty_tables_added_to_skipped(self):
        tables  = [("dbo", "T1")]
        session = self._make_session({"T1": ([("Id",)], [])})
        conn    = _MockMySQLConn()
        result  = migrate_all(session, conn, tables, _noop)
        assert "T1" in result["skipped"]
        assert result["total_rows"] == 0

    def test_migrated_dict_contains_correct_counts(self):
        tables  = [("dbo", "A"), ("dbo", "B")]
        session = self._make_session({
            "A": ([("Id",)], [(1,), (2,), (3,)]),
            "B": ([("Id",)], [(10,)]),
        })
        conn   = _MockMySQLConn()
        result = migrate_all(session, conn, tables, _noop)
        assert result["migrated"]["A"] == 3
        assert result["migrated"]["B"] == 1

    def test_error_in_one_table_does_not_abort_others(self):
        """Wenn T1 fehlschlägt, soll T2 trotzdem migriert werden."""
        import mysql.connector as mc

        tables = [("dbo", "T1"), ("dbo", "T2")]

        class _MixedCursor:
            """Wirft bei der ersten executemany, danach nicht mehr."""
            def __init__(self):
                self._count     = 0
                self.executed   = []
                self.many_calls = []
            def execute(self, sql, *a): self.executed.append(sql)
            def executemany(self, sql, batch):
                self._count += 1
                if self._count == 1:
                    raise mc.Error("insert fail")
                self.many_calls.append((sql, batch))
            def fetchall(self):
                return [("Id",)] if self._count == 0 else [(1,)]
            def close(self): pass

        class _MixedConn:
            def __init__(self):
                self._cur        = _MixedCursor()
                self.committed   = 0
                self.rolled_back = 0
            def cursor(self): return self._cur
            def commit(self): self.committed += 1
            def rollback(self): self.rolled_back += 1
            def close(self): pass

        # Session liefert col+data für T1 und T2
        session = self._make_session({
            "T1": ([("Id",)], [(1,)]),
            "T2": ([("Id",)], [(2,)]),
        })
        conn   = _MixedConn()
        result = migrate_all(session, conn, tables, _noop)

        assert len(result["errors"]) == 1
        assert "T1" in result["errors"][0]

    def test_errors_list_populated_on_failure(self):
        """Fehler beim INSERT sollen im errors-Feld landen."""
        import mysql.connector as mc

        tables  = [("dbo", "Bad")]
        session = self._make_session({"Bad": ([("Id",)], [(1,)])})

        class _FailOnInsert(_MockMySQLConn):
            """Cursor schlägt nur bei executemany fehl, nicht bei execute."""
            def cursor(self):
                class _C(_MockCursor):
                    def executemany(self, sql, batch):
                        raise mc.Error("insert oops")
                return _C()

        conn   = _FailOnInsert()
        result = migrate_all(session, conn, tables, _noop)
        assert len(result["errors"]) == 1
        assert "Bad" in result["errors"][0]

    def test_rollback_called_on_error(self):
        """Nach einem INSERT-Fehler muss rollback() aufgerufen werden."""
        import mysql.connector as mc

        tables  = [("dbo", "T")]
        session = self._make_session({"T": ([("Id",)], [(1,)])})

        class _FailOnInsert(_MockMySQLConn):
            def cursor(self):
                class _C(_MockCursor):
                    def executemany(self, sql, batch):
                        raise mc.Error("insert fail")
                return _C()

        conn = _FailOnInsert()
        migrate_all(session, conn, tables, _noop)
        assert conn.rolled_back >= 1

    def test_no_tables_returns_zero_rows(self):
        session = _MockSession(fetchall_results=[[]])
        conn    = _MockMySQLConn()
        result  = migrate_all(session, conn, [], _noop)
        assert result["total_rows"] == 0
        assert result["errors"]     == []
        assert result["skipped"]    == []

    def test_result_keys_present(self):
        session = _MockSession(fetchall_results=[[]])
        conn    = _MockMySQLConn()
        result  = migrate_all(session, conn, [], _noop)
        for key in ("total_rows", "skipped", "errors", "migrated"):
            assert key in result

    def test_fk_checks_disabled_and_re_enabled(self):
        """SET FOREIGN_KEY_CHECKS = 0 und = 1 müssen beide aufgerufen werden."""
        session = self._make_session({"T": ([("Id",)], [(1,)])})
        conn    = _MockMySQLConn()
        migrate_all(session, conn, [("dbo", "T")], _noop)
        sqls = " ".join(stmt[0] for stmt in conn._cur.executed)
        assert "FOREIGN_KEY_CHECKS = 0" in sqls
        assert "FOREIGN_KEY_CHECKS = 1" in sqls

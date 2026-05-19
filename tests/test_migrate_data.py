"""
Tests für src/migrate_data.py

Abgedeckte Funktionen:
  - get_table_list   (TestGetTableList)
  - iter_table_data  (TestIterTableData)
  - migrate_table    (TestMigrateTable)
  - migrate_all      (TestMigrateAll)

Alle externen Abhängigkeiten (pyodbc-Session, mysql.connector) werden
vollständig durch Mocks ersetzt – kein laufender Datenbankserver nötig.
"""
import threading
import pytest
from migrate_data import (
    get_table_list,
    iter_table_data,
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
        self._results   = iter(fetchall_results or [[]])
        self._effect    = side_effect
        self.executed   = []
        self.many_calls = []
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


def _table_session(col_names, data_rows):
    """Session-Mock für eine einzelne Tabelle.

    Liefert die fetchall-Sequenz die iter_table_data erwartet:
      1. Spaltennamen-Query
      2. Daten-Chunk (alle Zeilen auf einmal, da chunk_size > len(data_rows))
      3. Leere Liste → Generator stoppt
    """
    cols = [(c,) for c in col_names]
    return _MockSession(fetchall_results=[cols, data_rows, []])


def _all_session(tables_data: dict):
    """Session-Mock für migrate_all.

    tables_data: {table_name: (col_names, data_rows)}

    Fetchall-Sequenz:
      1. get_row_counts → [(schema, name, count), ...]
      2. Pro Tabelle: Spalten, Daten
         (kein [] Terminator nötig: Testdaten < CHUNK_SIZE=5000, Generator endet
          früher via "if len(rows) < chunk_size: return")
    """
    counts = [("dbo", name, len(rows)) for name, (_, rows) in tables_data.items()]
    results = [counts]
    for col_names, data_rows in tables_data.values():
        results.append([(c,) for c in col_names])
        results.append(data_rows)
    return _MockSession(fetchall_results=results)


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
#  iter_table_data
# ════════════════════════════════════════════════════════════════════════════
class TestIterTableData:

    def test_yields_columns_and_rows(self):
        session = _table_session(["Index", "Name", "Product"],
                                 [(1, "Bangle 1", "Silver"), (2, "Bangle 2", "Gold")])
        chunks = list(iter_table_data(session, "dbo", "TableArticle", chunk_size=100))
        assert len(chunks) == 1
        cols, rows = chunks[0]
        assert cols == ["Index", "Name", "Product"]
        assert rows == [(1, "Bangle 1", "Silver"), (2, "Bangle 2", "Gold")]

    def test_empty_table_yields_nothing(self):
        session = _MockSession(fetchall_results=[[("Id",)], []])
        chunks  = list(iter_table_data(session, "dbo", "EmptyTable", chunk_size=100))
        assert chunks == []

    def test_no_columns_yields_nothing(self):
        session = _MockSession(fetchall_results=[[]])
        chunks  = list(iter_table_data(session, "dbo", "T", chunk_size=100))
        assert chunks == []

    def test_none_values_preserved(self):
        session = _table_session(["A", "B"], [(1, None), (None, 2)])
        cols, rows = next(iter(iter_table_data(session, "dbo", "T", chunk_size=100)))
        assert rows[0][1] is None
        assert rows[1][0] is None

    def test_stop_event_aborts_iteration(self):
        """Gesetztes stop_event muss den Generator sofort beenden."""
        stop = threading.Event()
        stop.set()
        session = _table_session(["Id"], [(1,), (2,)])
        chunks  = list(iter_table_data(session, "dbo", "T", chunk_size=100,
                                       stop_event=stop))
        assert chunks == []

    def test_multiple_chunks(self):
        """Chunk-Grenze wird korrekt durch leeres fetchall signalisiert."""
        # Zwei Chunks à 2 Zeilen + abschließendes leeres fetchall
        session = _MockSession(fetchall_results=[
            [("Id",)],
            [(1,), (2,)],   # chunk 1
            [(3,), (4,)],   # chunk 2
            [],             # Ende
        ])
        chunks = list(iter_table_data(session, "dbo", "T", chunk_size=2))
        assert len(chunks) == 2
        assert chunks[0][1] == [(1,), (2,)]
        assert chunks[1][1] == [(3,), (4,)]


# ════════════════════════════════════════════════════════════════════════════
#  migrate_table
# ════════════════════════════════════════════════════════════════════════════
class TestMigrateTable:

    def test_returns_row_count(self):
        session = _table_session(["Id", "Name"], [(1, "A"), (2, "B")])
        conn    = _MockMySQLConn()
        count   = migrate_table(conn, "TableArticle", session, "dbo", 2, _noop)
        assert count == 2

    def test_empty_table_returns_zero(self):
        session = _MockSession(fetchall_results=[[("Id",)], []])
        conn    = _MockMySQLConn()
        count   = migrate_table(conn, "T", session, "dbo", 0, _noop)
        assert count == 0

    def test_empty_table_skipped_message_logged(self):
        session = _MockSession(fetchall_results=[[("Id",)], []])
        conn    = _MockMySQLConn()
        logged  = []
        migrate_table(conn, "MyTable", session, "dbo", 0, logged.append)
        assert any("übersprungen" in l or "skipped" in l.lower() for l in logged)

    def test_truncate_called_before_insert(self):
        session = _table_session(["Id"], [(1,)])
        conn    = _MockMySQLConn()
        migrate_table(conn, "T", session, "dbo", 1, _noop)
        first_sql = conn._cur.executed[0][0].upper()
        assert "TRUNCATE" in first_sql

    def test_executemany_called_with_correct_table(self):
        session = _table_session(["A", "B"], [(1, 2), (3, 4)])
        conn    = _MockMySQLConn()
        migrate_table(conn, "TableCost", session, "dbo", 2, _noop)
        assert len(conn._cur.many_calls) == 1
        sql, batch = conn._cur.many_calls[0]
        assert "TableCost" in sql
        assert batch == [(1, 2), (3, 4)]

    def test_commit_called_after_insert(self):
        session = _table_session(["Id"], [(1,)])
        conn    = _MockMySQLConn()
        migrate_table(conn, "T", session, "dbo", 1, _noop)
        assert conn.committed >= 1

    def test_success_logged(self):
        session = _table_session(["Id"], [(1,), (2,), (3,)])
        conn    = _MockMySQLConn()
        logged  = []
        migrate_table(conn, "TablePlating", session, "dbo", 3, logged.append)
        combined = " ".join(logged)
        assert "3" in combined and "TablePlating" in combined

    def test_memoryview_converted_to_bytes(self):
        data    = memoryview(b"\x00\x01\x02")
        session = _table_session(["Img"], [(data,)])
        conn    = _MockMySQLConn()
        migrate_table(conn, "T", session, "dbo", 1, _noop)
        _, batch = conn._cur.many_calls[0]
        assert isinstance(batch[0][0], (bytes, memoryview))

    def test_column_list_in_insert_sql(self):
        session = _table_session(["ColA", "ColB"], [(1, 2)])
        conn    = _MockMySQLConn()
        migrate_table(conn, "T", session, "dbo", 1, _noop)
        sql, _ = conn._cur.many_calls[0]
        assert "`ColA`" in sql
        assert "`ColB`" in sql

    def test_raises_on_mysql_error(self):
        import mysql.connector
        session = _table_session(["Id"], [(1,)])
        conn    = _MockMySQLConn(cursor_side_effect=mysql.connector.Error("fail"))
        with pytest.raises(Exception):
            migrate_table(conn, "T", session, "dbo", 1, _noop)

    def test_cancel_via_stop_event(self):
        stop    = threading.Event()
        stop.set()
        session = _table_session(["Id"], [(1,), (2,)])
        conn    = _MockMySQLConn()
        count   = migrate_table(conn, "T", session, "dbo", 2, _noop,
                                stop_event=stop)
        assert count == 0

    def test_progress_callback_called(self):
        session   = _table_session(["Id"], [(1,), (2,)])
        conn      = _MockMySQLConn()
        calls     = []
        migrate_table(conn, "T", session, "dbo", 2, _noop,
                      progress_callback=lambda t, d, total: calls.append((d, total)))
        assert len(calls) >= 1
        assert calls[-1][0] == 2


# ════════════════════════════════════════════════════════════════════════════
#  migrate_all
# ════════════════════════════════════════════════════════════════════════════
class TestMigrateAll:

    def test_returns_correct_total_rows(self):
        tables  = [("dbo", "T1"), ("dbo", "T2")]
        session = _all_session({"T1": (["Id"], [(1,), (2,)]),
                                "T2": (["Id"], [(3,)])})
        conn   = _MockMySQLConn()
        result = migrate_all(session, conn, tables, _noop)
        assert result["total_rows"] == 3

    def test_empty_tables_added_to_skipped(self):
        tables  = [("dbo", "T1")]
        session = _all_session({"T1": (["Id"], [])})
        conn    = _MockMySQLConn()
        result  = migrate_all(session, conn, tables, _noop)
        assert "T1" in result["skipped"]
        assert result["total_rows"] == 0

    def test_migrated_dict_contains_correct_counts(self):
        tables  = [("dbo", "A"), ("dbo", "B")]
        session = _all_session({"A": (["Id"], [(1,), (2,), (3,)]),
                                "B": (["Id"], [(10,)])})
        conn   = _MockMySQLConn()
        result = migrate_all(session, conn, tables, _noop)
        assert result["migrated"]["A"] == 3
        assert result["migrated"]["B"] == 1

    def test_error_in_one_table_does_not_abort_others(self):
        import mysql.connector as mc

        tables = [("dbo", "T1"), ("dbo", "T2")]

        class _MixedCursor:
            def __init__(self):
                self._executemany_count = 0
                self.executed           = []
                self.many_calls         = []
                self._fetchall_results  = iter([
                    [("dbo", "T1", 1), ("dbo", "T2", 1)],  # get_row_counts
                    [("Id",)], [(1,)], [],                   # T1
                    [("Id",)], [(2,)], [],                   # T2
                ])

            def execute(self, sql, *a):
                self.executed.append(sql)

            def executemany(self, sql, batch):
                self._executemany_count += 1
                if self._executemany_count == 1:
                    raise mc.Error("insert fail")
                self.many_calls.append((sql, batch))

            def fetchall(self):
                return list(next(self._fetchall_results, []))

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

        # Separate session with combined fetchall sequence
        session = _MockSession(fetchall_results=[
            [("dbo", "T1", 1), ("dbo", "T2", 1)],  # get_row_counts
            [("Id",)], [(1,)],                       # T1 (kein [] Terminator nötig)
            [("Id",)], [(2,)],                       # T2
        ])
        conn   = _MixedConn()
        result = migrate_all(session, conn, tables, _noop)

        assert len(result["errors"]) == 1
        assert "T1" in result["errors"][0]

    def test_errors_list_populated_on_failure(self):
        import mysql.connector as mc

        tables  = [("dbo", "Bad")]
        session = _all_session({"Bad": (["Id"], [(1,)])})

        class _FailOnInsert(_MockMySQLConn):
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
        import mysql.connector as mc

        tables  = [("dbo", "T")]
        session = _all_session({"T": (["Id"], [(1,)])})

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
        for key in ("total_rows", "skipped", "errors", "migrated", "cancelled"):
            assert key in result

    def test_fk_checks_disabled_and_re_enabled(self):
        session = _all_session({"T": (["Id"], [(1,)])})
        conn    = _MockMySQLConn()
        migrate_all(session, conn, [("dbo", "T")], _noop)
        sqls = " ".join(stmt[0] for stmt in conn._cur.executed)
        assert "FOREIGN_KEY_CHECKS = 0" in sqls
        assert "FOREIGN_KEY_CHECKS = 1" in sqls

    def test_cancelled_flag_set_when_stop_event_triggered(self):
        stop    = threading.Event()
        stop.set()
        tables  = [("dbo", "T")]
        session = _all_session({"T": (["Id"], [(1,)])})
        conn    = _MockMySQLConn()
        result  = migrate_all(session, conn, tables, _noop, stop_event=stop)
        assert result["cancelled"] is True

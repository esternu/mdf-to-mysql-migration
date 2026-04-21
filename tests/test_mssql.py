"""
Tests für src/mssql.py
Deckt ab: Verbindungsstring-Generierung, LDF-Suche, Treiber-Erkennung,
           MdfSession-Delegation und Schema-Parsing.
Externe Abhängigkeiten (pyodbc, subprocess, Dateisystem) werden gemockt.
"""
import os
import pytest
from mssql import (
    _build_conn_str,
    _find_ldf,
    _win_path,
    MdfSession,
    get_mssql_drivers,
    read_schema,
    PYODBC_OK,
)


# ════════════════════════════════════════════════════════════════════════════
#  _build_conn_str
# ════════════════════════════════════════════════════════════════════════════
class TestBuildConnStr:
    def test_driver_wrapped_in_braces(self):
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "DRIVER={ODBC Driver 17 for SQL Server}" in s

    def test_default_server_is_localdb(self, monkeypatch):
        monkeypatch.delenv("MSSQL_SERVER", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "MSSQLLocalDB" in s

    def test_mssql_server_env_overrides_localdb(self, monkeypatch):
        monkeypatch.setenv("MSSQL_SERVER", "myserver\\SQLEXPRESS")
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "myserver\\SQLEXPRESS" in s
        assert "MSSQLLocalDB" not in s

    def test_windows_auth_by_default(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "Trusted_Connection=yes" in s
        assert "UID=" not in s

    def test_sql_auth_when_user_and_pass_set(self, monkeypatch):
        monkeypatch.setenv("MSSQL_USER", "sa")
        monkeypatch.setenv("MSSQL_PASS", "Secret1!")
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "UID=sa" in s
        assert "PWD=Secret1!" in s
        assert "Trusted_Connection=no" in s
        assert "Trusted_Connection=yes" not in s

    def test_encrypt_added_for_driver_18(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 18 for SQL Server")
        assert "Encrypt=no" in s
        assert "TrustServerCertificate=yes" in s

    def test_no_encrypt_for_driver_17(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "Encrypt=no" not in s

    def test_database_appended_when_given(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server", "MyDB")
        assert "DATABASE=MyDB" in s

    def test_no_database_when_omitted(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert "DATABASE" not in s

    def test_result_ends_with_semicolon(self, monkeypatch):
        monkeypatch.delenv("MSSQL_USER", raising=False)
        monkeypatch.delenv("MSSQL_PASS", raising=False)
        s = _build_conn_str("ODBC Driver 17 for SQL Server")
        assert s.endswith(";")


# ════════════════════════════════════════════════════════════════════════════
#  _find_ldf
# ════════════════════════════════════════════════════════════════════════════
class TestFindLdf:
    def test_finds_log_suffix_ldf(self, tmp_path):
        mdf = tmp_path / "mydb.mdf"
        ldf = tmp_path / "mydb_log.ldf"
        mdf.touch()
        ldf.touch()
        assert _find_ldf(str(mdf)) == str(ldf)

    def test_finds_plain_ldf(self, tmp_path):
        mdf = tmp_path / "mydb.mdf"
        ldf = tmp_path / "mydb.ldf"
        mdf.touch()
        ldf.touch()
        assert _find_ldf(str(mdf)) == str(ldf)

    def test_prefers_log_suffix_over_plain(self, tmp_path):
        mdf = tmp_path / "mydb.mdf"
        mdf.touch()
        (tmp_path / "mydb_log.ldf").touch()
        (tmp_path / "mydb.ldf").touch()
        result = _find_ldf(str(mdf))
        assert result.endswith("mydb_log.ldf")

    def test_returns_none_when_no_ldf(self, tmp_path):
        mdf = tmp_path / "mydb.mdf"
        mdf.touch()
        assert _find_ldf(str(mdf)) is None

    def test_path_with_spaces(self, tmp_path):
        sub = tmp_path / "my database"
        sub.mkdir()
        mdf = sub / "Cockpit DB.mdf"
        ldf = sub / "Cockpit DB_log.ldf"
        mdf.touch()
        ldf.touch()
        assert _find_ldf(str(mdf)) == str(ldf)


# ════════════════════════════════════════════════════════════════════════════
#  _win_path
# ════════════════════════════════════════════════════════════════════════════
class TestWinPath:
    def test_forward_slashes_converted(self):
        result = _win_path("C:/Users/test/file.mdf")
        assert "/" not in result

    def test_backslashes_preserved(self):
        result = _win_path("C:\\Users\\test")
        assert "\\" in result


# ════════════════════════════════════════════════════════════════════════════
#  MdfSession
# ════════════════════════════════════════════════════════════════════════════
class TestMdfSession:
    def test_attributes_stored(self, mocker):
        conn    = mocker.MagicMock()
        session = MdfSession(conn, "TestDB", "ODBC Driver 18 for SQL Server", "/tmp/abc")
        assert session.db_name == "TestDB"
        assert session.driver  == "ODBC Driver 18 for SQL Server"
        assert session.tmp_dir == "/tmp/abc"

    def test_cursor_delegated_to_conn(self, mocker):
        conn    = mocker.MagicMock()
        session = MdfSession(conn, "DB", "drv", "/tmp")
        session.cursor()
        conn.cursor.assert_called_once()

    def test_close_delegated_to_conn(self, mocker):
        conn    = mocker.MagicMock()
        session = MdfSession(conn, "DB", "drv", "/tmp")
        session.close()
        conn.close.assert_called_once()


# ════════════════════════════════════════════════════════════════════════════
#  get_mssql_drivers
# ════════════════════════════════════════════════════════════════════════════
class TestGetMssqlDrivers:
    def test_returns_empty_when_pyodbc_unavailable(self, mocker):
        mocker.patch("mssql.PYODBC_OK", False)
        assert get_mssql_drivers() == []

    def test_modern_drivers_sorted_descending(self, mocker):
        mocker.patch("mssql.PYODBC_OK", True)
        mocker.patch("mssql.pyodbc.drivers", return_value=[
            "SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server",
        ])
        result = get_mssql_drivers()
        assert result[0] == "ODBC Driver 18 for SQL Server"
        assert result[1] == "ODBC Driver 17 for SQL Server"

    def test_legacy_driver_at_end(self, mocker):
        mocker.patch("mssql.PYODBC_OK", True)
        mocker.patch("mssql.pyodbc.drivers", return_value=[
            "ODBC Driver 17 for SQL Server",
            "SQL Server",
        ])
        result = get_mssql_drivers()
        assert result[-1] == "SQL Server"

    def test_only_legacy_driver(self, mocker):
        mocker.patch("mssql.PYODBC_OK", True)
        mocker.patch("mssql.pyodbc.drivers", return_value=["SQL Server"])
        result = get_mssql_drivers()
        assert result == ["SQL Server"]

    def test_no_matching_drivers_returns_empty(self, mocker):
        mocker.patch("mssql.PYODBC_OK", True)
        mocker.patch("mssql.pyodbc.drivers", return_value=["MySQL ODBC 8.0"])
        assert get_mssql_drivers() == []


# ════════════════════════════════════════════════════════════════════════════
#  read_schema
# ════════════════════════════════════════════════════════════════════════════
class TestReadSchema:
    """read_schema bekommt eine MdfSession mit gemocktem pyodbc-Cursor."""

    def _session(self, mocker, col_rows=None, pk_rows=None, fk_rows=None, view_rows=None):
        mock_cur  = mocker.MagicMock()
        mock_cur.fetchall.side_effect = [
            col_rows  or [],
            pk_rows   or [],
            fk_rows   or [],
            view_rows or [],
        ]
        mock_conn = mocker.MagicMock()
        mock_conn.cursor.return_value = mock_cur
        return MdfSession(mock_conn, "TestDB", "drv", "/tmp")

    def test_schema_keys_present(self, mocker):
        session = self._session(mocker)
        schema  = read_schema(session, lambda m: None)
        assert "tables" in schema and "views" in schema

    def test_single_table_parsed(self, mocker):
        col = ("dbo", "Users", "Id", 1, "NO", "int", None, None, None, None, 1)
        session = self._session(mocker, col_rows=[col])
        schema  = read_schema(session, lambda m: None)
        assert "dbo.Users" in schema["tables"]

    def test_column_fields_mapped(self, mocker):
        col = ("dbo", "Users", "Email", 2, "YES", "nvarchar", 255, None, None, None, 0)
        session = self._session(mocker, col_rows=[col])
        schema  = read_schema(session, lambda m: None)
        c = schema["tables"]["dbo.Users"]["columns"][0]
        assert c["name"]     == "Email"
        assert c["nullable"] is True
        assert c["type"]     == "nvarchar"
        assert c["max_len"]  == 255
        assert c["identity"] is False

    def test_identity_column_flagged(self, mocker):
        col = ("dbo", "Users", "Id", 1, "NO", "int", None, None, None, None, 1)
        session = self._session(mocker, col_rows=[col])
        schema  = read_schema(session, lambda m: None)
        assert schema["tables"]["dbo.Users"]["columns"][0]["identity"] is True

    def test_primary_key_assigned(self, mocker):
        col = ("dbo", "Users", "Id", 1, "NO", "int", None, None, None, None, 1)
        pk  = ("dbo", "Users", "Id")
        session = self._session(mocker, col_rows=[col], pk_rows=[pk])
        schema  = read_schema(session, lambda m: None)
        assert "Id" in schema["tables"]["dbo.Users"]["pk"]

    def test_foreign_key_assigned(self, mocker):
        col = ("dbo", "Orders", "UserId", 1, "NO", "int", None, None, None, None, 0)
        fk  = ("FK_Orders_Users", "dbo", "Orders", "UserId", "dbo", "Users", "Id")
        session = self._session(mocker, col_rows=[col], fk_rows=[fk])
        schema  = read_schema(session, lambda m: None)
        fks = schema["tables"]["dbo.Orders"]["fk"]
        assert len(fks) == 1
        assert fks[0]["name"]     == "FK_Orders_Users"
        assert fks[0]["from_col"] == "UserId"
        assert fks[0]["to_table"] == "Users"

    def test_view_parsed(self, mocker):
        vrow = ("dbo", "MyView", "CREATE VIEW [dbo].[MyView] AS SELECT 1")
        session = self._session(mocker, view_rows=[vrow])
        schema  = read_schema(session, lambda m: None)
        assert "dbo.MyView" in schema["views"]
        assert schema["views"]["dbo.MyView"]["name"] == "MyView"

    def test_multiple_tables_all_present(self, mocker):
        col_a = ("dbo", "A", "Id", 1, "NO", "int", None, None, None, None, 1)
        col_b = ("dbo", "B", "Id", 1, "NO", "int", None, None, None, None, 1)
        session = self._session(mocker, col_rows=[col_a, col_b])
        schema  = read_schema(session, lambda m: None)
        assert "dbo.A" in schema["tables"]
        assert "dbo.B" in schema["tables"]

    def test_log_called(self, mocker):
        session   = self._session(mocker)
        log_calls = []
        read_schema(session, log_calls.append)
        assert len(log_calls) > 0

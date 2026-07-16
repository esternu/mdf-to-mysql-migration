"""
Tests für src/deploy.py
Deckt ab: erfolgreiche Deployments, Fehlerbehandlung,
           Statement-Splitting und Verbindungsparameter.
mysql.connector wird vollständig gemockt.
"""
import pytest
import mysql.connector
from deploy import deploy_to_mysql


def _noop(msg: str) -> None:
    """Leerer Log-Callback für Tests die den Log-Inhalt nicht prüfen."""
    pass


class _MockCursor:
    """Einfacher Cursor-Mock mit konfigurierbaren Seiteneffekten."""

    def __init__(self, side_effects=None):
        self._effects   = iter(side_effects or [])
        self.calls: list = []

    def execute(self, stmt):
        self.calls.append(stmt)
        effect = next(self._effects, None)
        if isinstance(effect, Exception):
            raise effect

    def close(self):
        pass


class _MockConn:
    def __init__(self, cursor):
        self._cursor    = cursor
        self.committed  = 0
        self.closed     = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed += 1

    def close(self):
        self.closed = True


# ════════════════════════════════════════════════════════════════════════════
#  Erfolgreiche Deployments
# ════════════════════════════════════════════════════════════════════════════
class TestDeploySuccess:
    def test_single_statement_executed(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("CREATE TABLE t (id INT);", "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 1

    def test_multiple_statements_all_executed(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        ddl = "CREATE TABLE a (id INT);\nCREATE TABLE b (id INT);\nCREATE TABLE c (id INT);"
        deploy_to_mysql(ddl, "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 3

    def test_commit_called_per_statement(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("S1;S2;S3;", "h", 3306, "u", "p", "db", _noop)
        assert conn.committed == 3

    def test_connection_closed_after_run(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("SELECT 1;", "h", 3306, "u", "p", "db", _noop)
        assert conn.closed is True

    def test_success_message_logged(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        log_lines: list = []
        deploy_to_mysql("SELECT 1;", "h", 3306, "u", "p", "db", log_lines.append)
        assert any("erfolgreich" in l for l in log_lines)

    def test_connect_message_logged(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        log_lines: list = []
        deploy_to_mysql("SELECT 1;", "192.168.1.1", 3306, "u", "p", "db", log_lines.append)
        assert any("192.168.1.1" in l for l in log_lines)


# ════════════════════════════════════════════════════════════════════════════
#  Verbindungsparameter
# ════════════════════════════════════════════════════════════════════════════
class TestConnectionParameters:
    def test_host_passed_correctly(self, mocker):
        mock_connect = mocker.patch(
            "mysql.connector.connect",
            return_value=_MockConn(_MockCursor()),
        )
        deploy_to_mysql("SELECT 1;", "192.168.2.159", 3306, "user", "pass", "db", _noop)
        assert mock_connect.call_args[1]["host"] == "192.168.2.159"

    def test_port_passed_correctly(self, mocker):
        mock_connect = mocker.patch(
            "mysql.connector.connect",
            return_value=_MockConn(_MockCursor()),
        )
        deploy_to_mysql("SELECT 1;", "h", 3307, "user", "pass", "db", _noop)
        assert mock_connect.call_args[1]["port"] == 3307

    def test_credentials_passed(self, mocker):
        mock_connect = mocker.patch(
            "mysql.connector.connect",
            return_value=_MockConn(_MockCursor()),
        )
        deploy_to_mysql("SELECT 1;", "h", 3306, "admin", "s3cr3t", "db", _noop)
        kw = mock_connect.call_args[1]
        assert kw["user"]     == "admin"
        assert kw["password"] == "s3cr3t"

    def test_utf8mb4_charset_set(self, mocker):
        mock_connect = mocker.patch(
            "mysql.connector.connect",
            return_value=_MockConn(_MockCursor()),
        )
        deploy_to_mysql("SELECT 1;", "h", 3306, "u", "p", "db", _noop)
        assert mock_connect.call_args[1]["charset"] == "utf8mb4"

    def test_unknown_database_falls_back_to_no_database(self, mocker):
        # TODO 1.2: Erst-Deploy - Ziel-DB existiert noch nicht (Fehler 1049).
        # Fallback: ohne database verbinden, das Voll-DDL macht CREATE+USE.
        err = mysql.connector.Error(msg="Unknown database 'TestDB'", errno=1049)
        conn = _MockConn(_MockCursor())
        mock_connect = mocker.patch(
            "mysql.connector.connect", side_effect=[err, conn],
        )
        log_lines: list = []
        deploy_to_mysql("CREATE DATABASE IF NOT EXISTS `TestDB`;",
                        "h", 3306, "u", "p", "TestDB", log_lines.append)
        assert mock_connect.call_count == 2
        assert "database" in mock_connect.call_args_list[0][1]
        assert "database" not in mock_connect.call_args_list[1][1]
        assert any("existiert noch nicht" in l for l in log_lines)

    def test_other_connect_errors_still_raise(self, mocker):
        # Nur 1049 loest den Fallback aus - Zugriffsfehler etc. weiterhin raise.
        err = mysql.connector.Error(msg="Access denied", errno=1045)
        mocker.patch("mysql.connector.connect", side_effect=err)
        with pytest.raises(mysql.connector.Error):
            deploy_to_mysql("SELECT 1;", "h", 3306, "u", "p", "db", _noop)

    def test_target_db_selected_on_connect(self, mocker):
        # Ohne database= schlagen ALTER/CREATE TABLE mit "No database
        # selected" fehl, sobald Tabellennamen nicht voll qualifiziert sind.
        mock_connect = mocker.patch(
            "mysql.connector.connect",
            return_value=_MockConn(_MockCursor()),
        )
        deploy_to_mysql("SELECT 1;", "h", 3306, "u", "p", "Cockpit_Datenbank", _noop)
        assert mock_connect.call_args[1]["database"] == "Cockpit_Datenbank"


# ════════════════════════════════════════════════════════════════════════════
#  Fehlerbehandlung
# ════════════════════════════════════════════════════════════════════════════
class TestErrorHandling:
    def test_error_logged_then_raised(self, mocker):
        err = mysql.connector.Error("Syntax error")
        cur  = _MockCursor(side_effects=[err])
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        log_lines: list = []
        # Fehler werden vor dem Raise geloggt, damit der Aufrufer Details sieht
        with pytest.raises(RuntimeError):
            deploy_to_mysql("BAD SQL;", "h", 3306, "u", "p", "db", log_lines.append)
        assert any("⚠" in l or "Fehler" in l for l in log_lines)

    def test_partial_failure_continues_execution(self, mocker):
        """Wenn Statement 2 von 3 fehlschlägt, werden trotzdem alle ausgeführt."""
        effects = [None, mysql.connector.Error("fail"), None]
        cur  = _MockCursor(side_effects=effects)
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        with pytest.raises(RuntimeError):
            deploy_to_mysql("S1;S2;S3;", "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 3   # alle drei versucht

    def test_error_count_in_log(self, mocker):
        effects = [mysql.connector.Error("e1"), mysql.connector.Error("e2")]
        cur  = _MockCursor(side_effects=effects)
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        log_lines: list = []
        with pytest.raises(RuntimeError):
            deploy_to_mysql("S1;S2;", "h", 3306, "u", "p", "db", log_lines.append)
        # Log-Zusammenfassung soll "2" erwähnen
        combined = " ".join(log_lines)
        assert "2" in combined

    def test_connection_closed_even_after_error(self, mocker):
        err  = mysql.connector.Error("fail")
        cur  = _MockCursor(side_effects=[err])
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        with pytest.raises(RuntimeError):
            deploy_to_mysql("BAD;", "h", 3306, "u", "p", "db", _noop)
        assert conn.closed is True

    def test_raises_runtime_error_with_failure_count(self, mocker):
        effects = [None, mysql.connector.Error("fail")]
        cur  = _MockCursor(side_effects=effects)
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        with pytest.raises(RuntimeError, match="1 von 2"):
            deploy_to_mysql("S1;S2;", "h", 3306, "u", "p", "db", _noop)

    def test_no_raise_when_all_succeed(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("S1;S2;", "h", 3306, "u", "p", "db", _noop)  # no raise


# ════════════════════════════════════════════════════════════════════════════
#  Statement-Splitting
# ════════════════════════════════════════════════════════════════════════════
class TestStatementSplitting:
    def test_empty_statements_filtered(self, mocker):
        """Leeranweisungen zwischen Semikolons werden ignoriert."""
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("S1;   ;  \n  ;S2;", "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 2

    def test_whitespace_only_ddl_runs_zero_statements(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("  ;  ;  ", "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 0

    def test_trailing_semicolon_not_counted(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        deploy_to_mysql("CREATE TABLE t (id INT);", "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 1   # nicht 2

    def test_newlines_between_statements_ok(self, mocker):
        cur  = _MockCursor()
        conn = _MockConn(cur)
        mocker.patch("mysql.connector.connect", return_value=conn)

        ddl = "\nCREATE TABLE a (id INT);\n\nCREATE TABLE b (id INT);\n"
        deploy_to_mysql(ddl, "h", 3306, "u", "p", "db", _noop)
        assert len(cur.calls) == 2

from src.audit_triggers import generate_audit_triggers


_SCHEMA = {
    "tables": {
        "dbo.Bath": {
            "schema": "dbo", "name": "Bath",
            "columns": [
                {"name": "Id",     "pos": 1, "nullable": False, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": True},
                {"name": "Name",   "pos": 2, "nullable": True, "type": "nvarchar",
                 "max_len": 100, "precision": None, "scale": None,
                 "default": None, "identity": False},
                {"name": "Editor", "pos": 3, "nullable": True, "type": "nvarchar",
                 "max_len": 50, "precision": None, "scale": None,
                 "default": None, "identity": False},
            ],
            "pk": ["Id"],
            "fk": [],
        },
        "dbo.row_locks": {
            "schema": "dbo", "name": "row_locks",
            "columns": [
                {"name": "Id", "pos": 1, "nullable": False, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": True},
            ],
            "pk": ["Id"],
            "fk": [],
        },
        "dbo.NoEditor": {
            "schema": "dbo", "name": "NoEditor",
            "columns": [
                {"name": "Id",    "pos": 1, "nullable": False, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": True},
                {"name": "Value", "pos": 2, "nullable": True, "type": "int",
                 "max_len": None, "precision": None, "scale": None,
                 "default": None, "identity": False},
            ],
            "pk": ["Id"],
            "fk": [],
        },
    },
    "views": {},
}


class TestGenerateAuditTriggers:
    def test_audit_log_table_created(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert "CREATE TABLE `TableAuditLog`" in sql
        assert "`OldValue`  JSON NULL" in sql
        assert "`NewValue`  JSON NULL" in sql

    def test_delimiter_wrapping(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert sql.startswith("-- Generiert von mdf-to-mysql-migration")
        assert "DELIMITER $$" in sql
        assert sql.rstrip().endswith("DELIMITER ;")

    def test_excludes_default_tables(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert "trg_row_locks_Audit_Insert" not in sql
        assert "trg_TableAuditLog_Audit_Insert" not in sql

    def test_excludes_custom_tables(self):
        sql = generate_audit_triggers(_SCHEMA, exclude_tables=["Bath"])
        assert "trg_Bath_Audit_Insert" not in sql
        assert "trg_NoEditor_Audit_Insert" in sql

    def test_insert_trigger_with_editor(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert "CREATE TRIGGER `trg_Bath_Audit_Insert`" in sql
        assert "AFTER INSERT ON `Bath`" in sql
        assert "NEW.`Editor`" in sql
        assert "JSON_OBJECT('Id', NEW.`Id`, 'Name', NEW.`Name`, 'Editor', NEW.`Editor`)" in sql

    def test_delete_trigger_with_editor(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert "CREATE TRIGGER `trg_Bath_Audit_Delete`" in sql
        assert "AFTER DELETE ON `Bath`" in sql
        assert "OLD.`Editor`" in sql

    def test_update_trigger_diffs_only_changed_columns(self):
        sql = generate_audit_triggers(_SCHEMA)
        assert "CREATE TRIGGER `trg_Bath_Audit_Update`" in sql
        assert "IF NOT (OLD.`Name` <=> NEW.`Name`) THEN" in sql
        assert "JSON_SET(@audit_old, '$.Name', OLD.`Name`)" in sql
        assert "@audit_changed = 1" in sql
        assert "IF @audit_changed = 1 THEN" in sql

    def test_update_trigger_includes_pk_for_row_identification(self):
        sql = generate_audit_triggers(_SCHEMA)
        # PK-Spalte wird initial gesetzt, aber nicht in der Diff-Schleife verglichen
        assert "SET @audit_old = JSON_OBJECT('Id', OLD.`Id`);" in sql
        assert "SET @audit_new = JSON_OBJECT('Id', NEW.`Id`);" in sql
        assert "IF NOT (OLD.`Id` <=> NEW.`Id`) THEN" not in sql

    def test_no_editor_column_uses_null(self):
        sql = generate_audit_triggers(_SCHEMA)
        # NoEditor-Tabelle hat keine Editor-Spalte -> ChangedBy = NULL
        start = sql.index("CREATE TRIGGER `trg_NoEditor_Audit_Insert`")
        end   = sql.index("END$$", start)
        section = sql[start:end]
        assert "VALUES ('NoEditor', 'INSERT', NULL," in section

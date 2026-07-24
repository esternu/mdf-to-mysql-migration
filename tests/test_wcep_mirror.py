"""
Tests für src/wcep_mirror.py (Issue #60).

Prüft, ob beim DDL-Generieren die kanonische Schema-Datei
Cockpit_DatenBank.sql (und die Audit-Trigger-Datei) tatsächlich in den
WCEP-Schema-Ordner geschrieben werden – und ob die Spiegelung für andere
Zieldatenbanken bzw. ohne WCEP-Ordner korrekt unterbleibt.
"""
import os
import pytest
from wcep_mirror import mirror_to_wcep_schema, WCEP_TARGET_DB
from paths import WCEP_SCHEMA_FILENAME


class TestMirrorToWcepSchema:
    def test_creates_canonical_schema_file(self, tmp_path):
        # Kernfall: Datei wird tatsaechlich erzeugt und heisst korrekt.
        written = mirror_to_wcep_schema(
            WCEP_TARGET_DB, "CREATE TABLE `T` (`Id` INT);",
            "-- audit --", "audit_triggers_Cockpit_Datenbank.sql",
            schema_dir=str(tmp_path),
        )
        schema_file = tmp_path / WCEP_SCHEMA_FILENAME
        assert schema_file.exists(), "Cockpit_DatenBank.sql wurde NICHT erstellt"
        assert str(schema_file) in written
        assert schema_file.read_text(encoding="utf-8") == "CREATE TABLE `T` (`Id` INT);"

    def test_creates_audit_file_too(self, tmp_path):
        mirror_to_wcep_schema(
            WCEP_TARGET_DB, "ddl", "AUDIT-SQL",
            "audit_triggers_Cockpit_Datenbank.sql", schema_dir=str(tmp_path),
        )
        audit_file = tmp_path / "audit_triggers_Cockpit_Datenbank.sql"
        assert audit_file.exists()
        assert audit_file.read_text(encoding="utf-8") == "AUDIT-SQL"

    def test_returns_both_written_paths(self, tmp_path):
        written = mirror_to_wcep_schema(
            WCEP_TARGET_DB, "ddl", "audit",
            "audit_triggers_Cockpit_Datenbank.sql", schema_dir=str(tmp_path),
        )
        assert len(written) == 2

    def test_skipped_for_other_database(self, tmp_path):
        # Andere Zieldatenbank -> keine Spiegelung, keine Datei
        written = mirror_to_wcep_schema(
            "Some_Other_DB", "ddl", "audit",
            "audit_triggers_Some_Other_DB.sql", schema_dir=str(tmp_path),
        )
        assert written == []
        assert list(tmp_path.iterdir()) == []

    def test_skipped_when_wcep_dir_missing(self, tmp_path):
        # WCEP-Repo nicht ausgecheckt -> Ordner existiert nicht -> nichts tun
        missing = tmp_path / "does_not_exist"
        written = mirror_to_wcep_schema(
            WCEP_TARGET_DB, "ddl", "audit",
            "audit_triggers_Cockpit_Datenbank.sql", schema_dir=str(missing),
        )
        assert written == []
        assert not missing.exists()

    def test_overwrites_existing_file(self, tmp_path):
        # Regenerieren muss die alte (veraltete) Datei ersetzen.
        stale = tmp_path / WCEP_SCHEMA_FILENAME
        stale.write_text("VERALTET", encoding="utf-8")
        mirror_to_wcep_schema(
            WCEP_TARGET_DB, "NEU", "audit",
            "audit_triggers_Cockpit_Datenbank.sql", schema_dir=str(tmp_path),
        )
        assert stale.read_text(encoding="utf-8") == "NEU"

    def test_log_callback_invoked(self, tmp_path):
        logs = []
        mirror_to_wcep_schema(
            WCEP_TARGET_DB, "ddl", "audit",
            "audit_triggers_Cockpit_Datenbank.sql",
            log=logs.append, schema_dir=str(tmp_path),
        )
        assert any("WCEP Schema" in l for l in logs)

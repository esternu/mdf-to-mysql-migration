"""
Tests für src/config_utils.py (Profilauswahl der Headless-Runner, TODO 3.1).
"""
import pytest
from config_utils import select_profile


_CFG = {
    "Standard":        {"mysql_db": "Cockpit_Datenbank"},
    "Cockpit":         {"mysql_db": "Cockpit_Datenbank"},
    "ProductionTable": {"mysql_db": "Cockpit_Production_Datenbank"},
}


class TestSelectProfile:
    def test_default_is_first_profile(self):
        name, cfg = select_profile(_CFG, None)
        assert name == "Standard"
        assert cfg["mysql_db"] == "Cockpit_Datenbank"

    def test_named_profile_selected(self):
        name, cfg = select_profile(_CFG, "ProductionTable")
        assert name == "ProductionTable"
        assert cfg["mysql_db"] == "Cockpit_Production_Datenbank"

    def test_unknown_profile_raises_with_available_list(self):
        with pytest.raises(ValueError) as exc:
            select_profile(_CFG, "gibtsnicht")
        msg = str(exc.value)
        assert "gibtsnicht" in msg
        assert "Standard" in msg and "ProductionTable" in msg

    def test_empty_config_raises(self):
        with pytest.raises(ValueError):
            select_profile({}, None)

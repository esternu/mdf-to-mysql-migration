"""
Gemeinsame Konfigurations-Hilfen für die Headless-Runner.
"""
from typing import Dict, Optional, Tuple


def select_profile(all_cfg: Dict[str, dict], name: Optional[str] = None) -> Tuple[str, dict]:
    """Wählt ein Profil aus der config.json-Struktur.

    Parameters
    ----------
    all_cfg : dict   – kompletter Inhalt der config.json ({profilname: einstellungen})
    name    : str    – gewünschtes Profil; None = erstes Profil (bisheriges Verhalten)

    Returns
    -------
    (profilname, einstellungen)

    Raises
    ------
    ValueError – wenn config.json leer ist oder das Profil nicht existiert
                 (Meldung enthält die verfügbaren Profilnamen).
    """
    if not all_cfg:
        raise ValueError("config.json enthaelt keine Profile.")
    if name is None:
        first = next(iter(all_cfg))
        return first, all_cfg[first]
    if name in all_cfg:
        return name, all_cfg[name]
    available = ", ".join(all_cfg.keys())
    raise ValueError(
        f"Profil '{name}' nicht gefunden. Verfuegbare Profile: {available}"
    )

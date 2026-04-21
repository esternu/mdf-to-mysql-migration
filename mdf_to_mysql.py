"""
MDF → MySQL Migration Tool – Einstiegspunkt.
Lädt die tkinter-Oberfläche aus src/ui.py.
"""
# Windows: scharfe Darstellung auf HiDPI-Monitoren
try:
    from ctypes import windll
    windll.shcore.SetProcessDpiAwareness(1)
except Exception:
    pass

import sys
import os

# src/ zum Suchpfad hinzufügen damit alle Untermodule gefunden werden
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from ui import App  # noqa: E402

if __name__ == "__main__":
    App().mainloop()

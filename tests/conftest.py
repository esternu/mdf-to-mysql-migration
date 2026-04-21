"""
Pytest-Konfiguration für das MDF-to-MySQL Projekt.
Fügt src/ dem Python-Suchpfad hinzu, damit alle Module ohne Installation
direkt importierbar sind.
ui.py wird bewusst NICHT importiert (benötigt tkinter / Display).
"""
import os
import sys

# src/ voranstellen, damit transform, mssql, deploy, paths importierbar sind
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

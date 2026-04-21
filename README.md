# MDF → MySQL Migration Tool

Windows-GUI-Tool zum Migrieren von SQL Server `.mdf` Datenbankdateien auf einen MySQL-Server (z.B. Synology NAS).

## Funktionen

- `.mdf`-Datei per Dateidialog laden (via SQL Server LocalDB, Original bleibt unverändert)
- Tabellen, Spalten, Primary Keys, Foreign Keys und Views automatisch einlesen
- SQL Server-Datentypen in MySQL-Typen konvertieren
- Views topologisch sortieren (Abhängigkeiten werden zuerst erstellt)
- T-SQL-Konstrukte automatisch übersetzen (`STRING_AGG`, `OUTER APPLY`, Typnamen, …)
- MySQL-DDL-Vorschau anzeigen und bearbeiten
- Direkt auf Ziel-MySQL (Synology) deployen oder DDL als `.sql` speichern
- Konfigurationsprofile speichern und laden (`config.json`)
- Headless-Betrieb ohne GUI für automatisierte Läufe (`run_headless.py`)

## Voraussetzungen

| Komponente | Download |
|---|---|
| Python 3.8+ | https://python.org |
| SQL Server LocalDB | https://aka.ms/sqllocaldb |
| Python-Pakete | `install_deps.bat` ausführen |

## Schnellstart

1. `install_deps.bat` ausführen (einmalig)
2. `start_tool.bat` doppelklicken
3. Tab **1 · Quelle** → `.mdf`-Datei wählen → „Schema lesen"
4. Tab **2 · Ziel** → Synology IP/Zugangsdaten eintragen → „Verbindung testen"
5. „DDL generieren" → Vorschau prüfen → „Auf MySQL deployen"

## Headless-Betrieb

Für automatisierte Migrationen ohne GUI (z.B. in Skripten oder CI):

```bash
py run_headless.py
```

Liest das erste Profil aus `config.json` und führt alle drei Schritte durch.
Das generierte SQL wird in `temp/last_generated.sql` abgelegt,
das Log in `../mdf-to-mysql-logs/migration_YYYYMMDD_HHMMSS.log`.

## Codestruktur

```
mdf-to-mysql-migration/
│
├── mdf_to_mysql.py          # Einstiegspunkt (GUI): DPI-Setup, sys.path, startet App
├── run_headless.py          # Einstiegspunkt (Headless): liest config.json, ruft src/ auf
│
├── src/                     # Quellcode-Module
│   ├── __init__.py          # Package-Marker
│   │
│   ├── paths.py             # Gemeinsame Pfad-Konstanten
│   │                        #   PROJECT_DIR, CFG_FILE, LOG_DIR, TEMP_DIR, LOG_FILE
│   │
│   ├── mssql.py             # SQL Server Zugriff
│   │                        #   get_mssql_drivers()        – ODBC-Treiber erkennen
│   │                        #   MdfSession                 – Wrapper um pyodbc.Connection
│   │                        #   attach_mdf()               – temporäre Kopie anhängen
│   │                        #   read_schema()              – Tabellen/Views/PKs/FKs lesen
│   │                        #   detach_and_cleanup()       – DB detachen, Kopie löschen
│   │
│   ├── transform.py         # SQL Server → MySQL Transformation
│   │                        #   TYPE_MAP / convert_type()  – Datentyp-Konvertierung
│   │                        #   mssql_name()               – [Name] → `Name`
│   │                        #   convert_default()          – DEFAULT-Werte übersetzen
│   │                        #   convert_view_sql()         – T-SQL-View → MySQL-View
│   │                        #   _convert_apply_to_join()   – OUTER APPLY → LEFT JOIN
│   │                        #   _topo_sort_views()         – Kahn-Algorithmus für Views
│   │                        #   generate_mysql_ddl()       – vollständiges DDL erzeugen
│   │
│   ├── deploy.py            # MySQL Deployment
│   │                        #   deploy_to_mysql()          – DDL auf Zielserver ausführen
│   │
│   └── ui.py                # tkinter GUI
│                            #   App (tk.Tk)                – 4-Tab-Oberfläche
│                            #     Tab 1: Quelle (.mdf)
│                            #     Tab 2: Ziel (MySQL)
│                            #     Tab 3: DDL-Vorschau
│                            #     Tab 4: Log
│                            #   Konfigurations-Management  – Profile speichern/laden
│
├── temp/                    # Generierte SQL-Dateien (nicht in Git eingecheckt)
│   └── last_generated.sql   # Zuletzt erzeugtes DDL-Script (nach jedem Lauf überschrieben)
│
├── config.json              # Verbindungsprofile (nicht in Git, Passwort Base64-kodiert)
├── install_deps.bat         # pip install pyodbc mysql-connector-python
├── start_tool.bat           # py mdf_to_mysql.py
└── .gitignore
```

### Modul-Abhängigkeiten

```
mdf_to_mysql.py ──► src/ui.py
                         │
                         ├──► src/paths.py
                         ├──► src/mssql.py     ──► pyodbc
                         ├──► src/transform.py
                         └──► src/deploy.py    ──► mysql-connector-python

run_headless.py  ──► src/paths.py
                 ──► src/mssql.py
                 ──► src/transform.py
                 ──► src/deploy.py
```

## Synology-Vorbereitung

- Im **Paket-Zentrum** → MariaDB 10 installieren
- In **phpMyAdmin** oder per SSH einen Benutzer mit Remote-Rechten anlegen:
  ```sql
  CREATE USER 'migration'@'%' IDENTIFIED BY 'sicheres_passwort';
  GRANT ALL PRIVILEGES ON *.* TO 'migration'@'%';
  FLUSH PRIVILEGES;
  ```

## Logs

Logs werden automatisch in `../mdf-to-mysql-logs/` abgelegt (ein Ordner über dem Projektverzeichnis).
Jeder Lauf erzeugt eine eigene Datei `migration_YYYYMMDD_HHMMSS.log`.
Die GUI zeigt den aktuellen Log-Pfad im Tab **4 · Log** an und öffnet den Ordner per Klick im Explorer.

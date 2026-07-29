# MDF → MySQL Migration Tool

Windows-GUI-Tool zum Migrieren von SQL Server `.mdf`-Datenbankdateien auf einen MySQL/MariaDB-Server (z. B. Synology NAS) — Schema **und** Daten, komplett oder inkrementell.

## Funktionen

**Schema**
- `.mdf`-Datei per Dateidialog laden (via SQL Server LocalDB — das Original bleibt unverändert, das Tool arbeitet auf einer temporären Kopie)
- Tabellen, Spalten, Primary Keys, Foreign Keys, Indexe (inkl. gefilterter UNIQUE-Indexe) und Views automatisch einlesen
- SQL-Server-Datentypen nach MySQL konvertieren (`NVARCHAR`→`VARCHAR`, `BIT`→`TINYINT(1)`, `TINYINT`→`TINYINT UNSIGNED`, …)
- T-SQL-Konstrukte in Views übersetzen (`STRING_AGG`→`GROUP_CONCAT`, `OUTER APPLY`→`LEFT JOIN`, `ISNULL`→`IFNULL`, String-Konkatenation `+`→`CONCAT()`, …)
- Views topologisch sortieren (abhängige Views werden nach ihren Basis-Views erstellt)
- MySQL-DDL als Vorschau anzeigen, bearbeiten und als `.sql` speichern

**Deployment**
- **Vollständig:** `DROP TABLE` + `CREATE TABLE` (Erstmigration)
- **Schema-Diff (inkrementell):** vergleicht MDF-Schema mit dem Live-MySQL-Schema und erzeugt nur die nötigen `ALTER TABLE`/`CREATE`-Anweisungen — bestehende Daten bleiben erhalten. Geänderte/neue **Views** (z. B. umbenannte View-Spalten) werden per `DROP VIEW`+`CREATE VIEW` mitsynchronisiert (Spaltensignatur-Vergleich; datenlos, daher unkritisch)
- **Umbenennungs-Erkennung:** entfernte + neue Spalte mit gleichem Typ wird als mögliche Umbenennung erkannt; nach Bestätigung werden die Werte kopiert und die alte Spalte entfernt
- **Dry-Run:** zeigt Diff und Datenumfang an, ohne etwas auszuführen

**Daten**
- Datenübertragung SQL Server → MySQL in Chunks (5 000 Zeilen), mit Fortschrittsanzeige und Abbrechen-Button
- Scope wählbar: alle Tabellen oder nur die vom Schema-Diff betroffenen
- **Checkpoint/Resume:** nach jeder Tabelle wird der Fortschritt gespeichert; nach einem Fehler setzt „Resume" dort fort

**Audit**
- Generator für MySQL-Audit-Trigger (`audit_triggers_<db>.sql`): pro Tabelle drei Row-Trigger (INSERT/UPDATE/DELETE) mit JSON-Delta-Logging in `TableAuditLog` — Ersatz für die MSSQL-seitigen Audit-Trigger/`ViewAuditChanges`
- Für die `Cockpit_Datenbank` wird die Trigger-Datei zusätzlich nach `../WCEP/Tools/schema/` gespiegelt

**Sonstiges**
- Konfigurationsprofile in `config.json` (Passwort Base64-kodiert)
- Headless-Betrieb ohne GUI (`run_headless.py`, `run_migrate_data.py`)
- Ausführliche Logs pro Lauf in `../mdf-to-mysql-logs/`

## Voraussetzungen

| Komponente | Download |
|---|---|
| Python 3.8+ | https://python.org |
| SQL Server LocalDB | https://aka.ms/sqllocaldb |
| Python-Pakete | `install_deps.bat` ausführen (`pyodbc`, `mysql-connector-python`) |

## Schnellstart

1. `install_deps.bat` ausführen (einmalig)
2. `start_tool.bat` doppelklicken
3. Tab **1 · Quelle** → `.mdf`-Datei wählen → **Schema lesen**
4. Tab **2 · Ziel** → Synology-IP/Zugangsdaten eintragen → **Verbindung testen**
5. **DDL generieren** → Vorschau im Tab 3 prüfen
6. **Auf MySQL deployen** — beim ersten Mal ohne Haken „Schema-Diff", danach inkrementell

Die Datei `audit_triggers_<db>.sql` enthält `DELIMITER`-Syntax und muss **manuell** per mysql-CLI eingespielt werden (nicht über den Deploy-Button).

## Headless-Betrieb

```bash
py run_headless.py        # Schema lesen → DDL generieren → deployen
py run_migrate_data.py    # Nur Daten migrieren (mit --dry-run / --resume)
```

Beide lesen das **erste** Profil aus `config.json`. Das generierte SQL landet in `temp/`, die Logs in `../mdf-to-mysql-logs/`.

## Codestruktur

```
mdf-to-mysql-migration/
│
├── mdf_to_mysql.py          # Einstiegspunkt GUI: DPI-Setup, sys.path, startet App
├── run_headless.py          # Einstiegspunkt headless: Schema → DDL → Deploy
├── run_migrate_data.py      # Einstiegspunkt headless: nur Datenmigration
│
├── src/
│   ├── paths.py             # Pfad-Konstanten (CFG_FILE, LOG_DIR, TEMP_DIR,
│   │                        #   CHECKPOINT_FILE, WCEP_SCHEMA_DIR)
│   │
│   ├── mssql.py             # SQL-Server-Zugriff
│   │                        #   get_mssql_drivers()   – ODBC-Treiber erkennen
│   │                        #   attach_mdf()          – temporäre Kopie anhängen
│   │                        #                           (Fallback ATTACH_REBUILD_LOG)
│   │                        #   read_schema()         – Tabellen/PKs/FKs/Indexe/Views
│   │                        #   detach_and_cleanup()  – DB detachen, Kopie löschen
│   │
│   ├── transform.py         # SQL Server → MySQL Übersetzung
│   │                        #   TYPE_MAP/convert_type()  – Datentypen
│   │                        #   convert_default()        – DEFAULT-Ausdrücke
│   │                        #   convert_view_sql()       – T-SQL-View → MySQL-View
│   │                        #   render_index_ddl()       – Indexe, inkl. Emulation
│   │                        #                              gefilterter UNIQUE-Indexe
│   │                        #   generate_mysql_ddl()     – komplettes DDL
│   │
│   ├── schema_diff.py       # Inkrementelles Deployment
│   │                        #   read_mysql_schema()          – Live-Schema lesen
│   │                        #   diff_schemas()               – MDF vs. MySQL vergleichen
│   │                        #   detect_rename_candidates()   – Spalten-Umbenennungen
│   │                        #   generate_diff_ddl()          – ALTER/CREATE-Skript
│   │
│   ├── migrate_data.py      # Datenmigration in Chunks
│   │                        #   iter_table_data()  – Chunk-Generator (SQL Server)
│   │                        #   migrate_table()    – TRUNCATE + INSERT je Tabelle
│   │                        #   migrate_all()      – Orchestrierung, Dry-Run,
│   │                        #                        Checkpoint/Resume, Whitelist
│   │
│   ├── audit_triggers.py    # MySQL-Audit-Trigger-Generator (JSON-Delta-Logging)
│   │
│   ├── deploy.py            # deploy_to_mysql() – DDL statementweise ausführen,
│   │                        #   Fehler sammeln, bei Fehlern RuntimeError
│   │
│   └── ui.py                # tkinter-GUI: 4 Tabs (Quelle/Ziel/DDL/Log),
│                            #   Status-LEDs, Fortschritt, Profile, Rename-Dialog
│
├── tests/                   # pytest-Suite (226 Tests, ohne DB-Verbindung lauffähig)
├── documentation/           # TODO.md, Reviews, PDF-Generatoren (benötigen reportlab)
├── temp/                    # Generierte SQL-Dateien + Checkpoint (nicht in Git)
├── config.json              # Verbindungsprofile (nicht in Git)
├── install_deps.bat         # pip install pyodbc mysql-connector-python
└── start_tool.bat           # py mdf_to_mysql.py
```

### Modul-Abhängigkeiten

```
mdf_to_mysql.py ──► src/ui.py ──┬──► src/mssql.py        ──► pyodbc
                                ├──► src/transform.py
                                ├──► src/schema_diff.py   ──► src/transform.py
                                ├──► src/migrate_data.py
                                ├──► src/audit_triggers.py
                                ├──► src/deploy.py        ──► mysql-connector-python
                                └──► src/paths.py

run_headless.py      ──► mssql, transform, deploy, paths
run_migrate_data.py  ──► mssql, migrate_data, paths
```

## Tests

```bash
py -m pytest -q --ignore=temp
```

Alle Tests laufen ohne SQL-Server- oder MySQL-Verbindung (Mocks).

## Synology-Vorbereitung

- Im **Paket-Zentrum** → MariaDB 10 installieren
- In **phpMyAdmin** oder per SSH einen Benutzer mit Remote-Rechten anlegen:
  ```sql
  CREATE USER 'migration'@'%' IDENTIFIED BY 'sicheres_passwort';
  GRANT ALL PRIVILEGES ON *.* TO 'migration'@'%';
  FLUSH PRIVILEGES;
  ```

## Logs

Jeder Lauf schreibt eine eigene Datei `migration_YYYYMMDD_HHMMSS.log` (Datenmigration: `data_migration_…`) nach `../mdf-to-mysql-logs/` — ein Ordner **über** dem Projektverzeichnis, damit Logs ein `git clean` überleben. Die GUI zeigt den aktuellen Pfad im Tab **4 · Log** und öffnet den Ordner per Klick im Explorer.

## Bekannte offene Punkte

Siehe [documentation/TODO.md](documentation/TODO.md) — priorisierte Liste aus dem Code-Review vom 2026-06-28 (u. a. verlorene `ON DELETE CASCADE`-Regeln, `LEN`→`CHAR_LENGTH`, stilles Entfernen von `TOP n`).

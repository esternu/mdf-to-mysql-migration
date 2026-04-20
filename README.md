# MDF → MySQL Migration Tool

Windows-GUI-Tool zum Migrieren von SQL Server `.mdf` Datenbankdateien auf einen MySQL-Server (z.B. Synology NAS).

## Funktionen

- `.mdf`-Datei per Dateidialog laden (via SQL Server LocalDB)
- Tabellen, Spalten, Primary Keys, Foreign Keys und Views automatisch einlesen
- SQL Server-Datentypen in MySQL-Typen konvertieren
- MySQL-DDL-Vorschau anzeigen und bearbeiten
- Direkt auf Ziel-MySQL (Synology) deployen oder DDL als `.sql` speichern

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

## Synology-Vorbereitung

- Im **Paket-Zentrum** → MariaDB 10 installieren
- In **phpMyAdmin** oder per SSH einen Benutzer mit Remote-Rechten anlegen:
  ```sql
  CREATE USER 'migration'@'%' IDENTIFIED BY 'sicheres_passwort';
  GRANT ALL PRIVILEGES ON *.* TO 'migration'@'%';
  FLUSH PRIVILEGES;
  ```

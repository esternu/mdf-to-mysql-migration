# TODO — Code-Review MDF-to-MySQL Migration Tool

**Stand:** 2026-06-28 · Branch `master` · alle 226 Tests grün
**Geprüft:** `src/` (alle Module), `run_headless.py`, `run_migrate_data.py`, Übersetzungs­funktionen gegen MSSQL-/MySQL-Syntax, Live-Datenbank auf Synology (`Cockpit_Datenbank`).

Jeder Punkt nennt: **Was** ist falsch, **wo** es steht, **warum** es wichtig ist und den **Lösungsvorschlag**. Reihenfolge = empfohlene Abarbeitung. Workflow wie gehabt: pro Punkt ein GitHub-Issue → Branch → PR.

---

## P1 — Kritisch (falsches Verhalten, betrifft die Produktion)

### 1.1 `ON DELETE CASCADE` geht bei allen Foreign Keys verloren
- **Wo:** `src/mssql.py` → FK-Abfrage (liest `delete_referential_action` nicht) und `src/transform.py` → `generate_mysql_ddl()` (schreibt keine `ON DELETE`/`ON UPDATE`-Klausel).
- **Symptom (verifiziert):** Alle 10 FKs in der Live-DB stehen auf `RESTRICT`. Das MSSQL-Original nutzt `ON DELETE CASCADE` (z. B. `FK_TableSolution_TablePlating`). Löscht man in WCEP ein Plating, erwartet die App eine Kaskade (`CASCADE_DEPENDENTS` in `api.js`) — in MySQL schlägt das Löschen stattdessen mit FK-Fehler fehl.
- **Fix:** In der FK-Query `fk.delete_referential_action_desc, fk.update_referential_action_desc` mitlesen; im DDL `ON DELETE CASCADE` / `ON UPDATE …` anhängen (NO_ACTION → weglassen). Auch `schema_diff.generate_diff_ddl()` (new_fks) anpassen. Danach einmalig die bestehenden FKs auf dem Synology-Server per `ALTER TABLE … DROP FOREIGN KEY / ADD CONSTRAINT … ON DELETE CASCADE` korrigieren.

### 1.2 Deploy auf frischen Server schlägt fehl (Regression aus Issue #27)
- **Wo:** `src/deploy.py` → `deploy_to_mysql()` verbindet jetzt immer mit `database=target_db`.
- **Symptom:** Beim allerersten Voll-Deploy existiert die Zieldatenbank noch nicht — die Verbindung scheitert mit Fehler 1049, obwohl das DDL selbst `CREATE DATABASE IF NOT EXISTS` + `USE` enthält. Vor dem #27-Fix funktionierte genau dieser Fall (und nur der).
- **Fix:** Verbindungsaufbau mit Fallback: erst mit `database=target_db` versuchen, bei Fehler 1049 ohne `database` verbinden (das Voll-DDL wählt die DB dann per `USE` selbst). Test ergänzen.

### 1.3 `LEN()` → `LENGTH()` ist die falsche MySQL-Funktion
- **Wo:** `src/transform.py` → `convert_view_sql()`, Zeile mit `\bLEN\s*\(` → `LENGTH(`.
- **Symptom:** T-SQL `LEN()` zählt **Zeichen**, MySQL `LENGTH()` zählt **Bytes**. Bei utf8mb4 liefert `LENGTH('Grün')` = 5, korrekt wäre 4. Alle Texte mit Umlauten/Sonderzeichen ergeben falsche Längen.
- **Fix:** Nach `CHAR_LENGTH(` übersetzen. Hinweis dokumentieren: T-SQL `LEN` ignoriert zusätzlich nachgestellte Leerzeichen (`LEN('ab ')` = 2, `CHAR_LENGTH('ab ')` = 3) — falls das relevant wird, `CHAR_LENGTH(RTRIM(…))`.

### 1.4 Chunk-Lesen ohne stabile Sortierung kann Zeilen doppeln/verlieren
- **Wo:** `src/migrate_data.py` → `iter_table_data()`: `ORDER BY (SELECT NULL) OFFSET … FETCH NEXT …`.
- **Symptom:** SQL Server garantiert bei `ORDER BY (SELECT NULL)` **keine** stabile Reihenfolge zwischen zwei Abfragen. Bei Tabellen > `CHUNK_SIZE` (5 000) können Chunks überlappen oder Lücken haben → still verlorene oder doppelte Zeilen.
- **Fix:** Eine einzige `SELECT *`-Abfrage öffnen und mit `cursor.fetchmany(chunk_size)` streamen (pyodbc kann das; kein OFFSET nötig). Alternativ `ORDER BY` über die PK-Spalten.

---

## P2 — Wichtig (stille Auslassungen der Übersetzung, Randfälle mit Datenverlust)

### 2.1 `warnings` in `convert_view_sql()` wird nie befüllt
- **Wo:** `src/transform.py` → `convert_view_sql()` legt `warnings: List[str] = []` an, gibt sie zurück — aber kein einziger Codepfad hängt etwas an. `generate_mysql_ddl()` würde sie als `-- ⚠`-Kommentare ins DDL schreiben.
- **Symptom:** Nicht übersetzbare T-SQL-Konstrukte laufen **stillschweigend** durch und knallen erst beim Deploy (oder schlimmer: verhalten sich falsch). Der Warnmechanismus existiert, ist aber tot.
- **Fix:** Erkennungs-Pass für bekannte Problemfälle ergänzen, mindestens: `CONVERT(`, `DATEADD(`, `DATEDIFF(`, `DATEPART(`, `FORMAT(`, `FULL OUTER JOIN`, `PIVOT`, `CROSS APPLY`-Reste, verbleibende `'…' + …`-Konkatenation (siehe 2.4). Je Fund eine Warnung anhängen.

### 2.2 `TOP n` wird ersatzlos gestrichen
- **Wo:** `src/transform.py` → `convert_view_sql()`: `re.sub(r'\bTOP\s+\d+\b', '', sql)`.
- **Symptom:** Eine View mit `SELECT TOP 10 …` liefert nach Migration **alle** Zeilen — stille Semantikänderung, keine Warnung, kein `LIMIT`.
- **Fix:** `TOP n` am Anfang des SELECT in `LIMIT n` am View-Ende übersetzen (MySQL-Views unterstützen LIMIT); wenn die Umstellung im Einzelfall zu komplex ist (`TOP n WITH TIES`, `TOP (expr)`), mindestens eine Warnung erzeugen.

### 2.3 `CONVERT()` hat vertauschte Argumente und wird nicht übersetzt
- **Wo:** `src/transform.py` → `convert_view_sql()` (keine Behandlung vorhanden).
- **Symptom:** T-SQL `CONVERT(NVARCHAR(10), col)` = *(Typ, Ausdruck)*, MySQL `CONVERT(col, CHAR(10))` = *(Ausdruck, Typ)*. Unübersetzt entsteht gültig aussehendes, aber falsches SQL — MySQL interpretiert den Typnamen als Spaltenausdruck.
- **Fix:** Regex-Übersetzung `CONVERT(typ, expr)` → `CAST(expr AS typ)` (2-Argument-Form); 3-Argument-Form mit Style-Nummer (`CONVERT(VARCHAR, datum, 104)`) als Warnung flaggen (Datumsformate müssen manuell zu `DATE_FORMAT()` werden).

### 2.4 String-Konkatenation: unerkannte `+`-Ketten korrumpieren still Daten
- **Wo:** `src/transform.py` → `_convert_string_concat()` (CAST-Operand-Regex erlaubt nur 1 Klammer-Ebene).
- **Symptom:** Eine Kette wie `'x' + CAST(ROUND(a/b, 2) AS CHAR)` (2 Ebenen) wird nicht erkannt → das `+` bleibt stehen → MySQL rechnet **numerisch**: `'x' + …` ergibt `0` bzw. Unsinn — ohne Fehlermeldung.
- **Fix:** Nach der Konvertierung prüfen, ob noch Muster `'…'\s*\+` bzw. `\+\s*'…'` übrig sind → Warnung (siehe 2.1). Optional: Operand-Regex auf 2 Nesting-Ebenen erweitern.

### 2.5 `BINARY(n)` verliert seine Länge
- **Wo:** `src/transform.py` → `convert_type()`: `max_len` wird nur auf VARCHAR/CHAR angewandt; `binary` → `"BINARY"` ohne Länge = `BINARY(1)` in MySQL.
- **Symptom:** Eine `binary(16)`-Spalte (z. B. Hash) würde als `BINARY(1)` angelegt → Daten-Trunkierung beim Insert. (Cockpit-DB aktuell nicht betroffen — `Picture` ist `varbinary(max)` → LONGBLOB, korrekt.)
- **Fix:** `BINARY`/`VARBINARY(n)` mit `max_len` behandeln: `binary(n)` → `BINARY(n)`, `varbinary(n≤65532)` → `VARBINARY(n)`, `varbinary(max)` → `LONGBLOB` (wie bisher).

### 2.6 `datetime2(scale)` / `time(scale)`: Präzision wird ignoriert
- **Wo:** `src/transform.py` → `TYPE_MAP`: `datetime2` → immer `DATETIME(6)`, `time` → `TIME` ohne fsp.
- **Symptom:** `datetime2(0)` (so in allen Cockpit-Tabellen!) wird zu `DATETIME(6)` — funktioniert, speichert aber Mikrosekunden, die die Quelle nie hatte; Vergleiche/Gruppierungen können abweichen. Umgekehrt verliert `time(3)` seine Millisekunden.
- **Fix:** `scale` aus dem Schema-Dict nutzen: `datetime2(s)` → `DATETIME(s)`, `time(s)` → `TIME(s)`. Achtung: Diff-Normalisierung (`schema_diff._normalize_mysql_type`) mitziehen, sonst Phantom-Typänderungen wie bei TINYINT (#31).

### 2.7 `convert_default()` zerbricht an `N'…'` und verschachtelten Klammern
- **Wo:** `src/transform.py` → `convert_default()`: `strip("()")` entfernt zeichenweise, `strip("'\"")` kennt kein `N`-Präfix.
- **Symptom:** MSSQL-Default `(N'Standard')` wird zu `'N'Standard'` (kaputtes Literal → Deploy-Fehler). `('Wert (intern)')` verliert die schließende Klammer. Zusätzlich: `DEFAULT CURRENT_TIMESTAMP` auf einer `DATETIME(6)`-Spalte ist in striktem MySQL 8 ungültig (braucht `CURRENT_TIMESTAMP(6)`) — MariaDB toleriert es, reine MySQL-Server nicht.
- **Fix:** Klammern paarweise (balanciert) entfernen statt `strip()`; `N'…'` → `'…'`; bei `CURRENT_TIMESTAMP` die fsp der Zielspalte anhängen.

### 2.8 `OUTER APPLY`-Fallback erzeugt ungültiges SQL
- **Wo:** `src/transform.py` → `_convert_apply_to_join()`, Fallback-Zweig (`# Fallback: Body unverändert übernehmen`).
- **Symptom:** Wenn die WHERE-Korrelation nicht erkannt wird, entsteht `LEFT JOIN (…) AS alias` **ohne ON-Klausel** → Syntaxfehler beim Deploy (MySQL verlangt ON bei LEFT JOIN).
- **Fix:** Fallback mit `ON 1=1` ergänzen **und** Warnung ausgeben (Semantik muss manuell geprüft werden).

### 2.9 Composite Foreign Keys erzeugen Namenskollision
- **Wo:** `src/mssql.py` (ein FK-Eintrag **pro Spaltenpaar**) + `src/transform.py`/`schema_diff.py` (ein `ADD CONSTRAINT` pro Eintrag).
- **Symptom:** Ein mehrspaltiger FK ergäbe zwei `ADD CONSTRAINT` mit identischem Namen → zweiter schlägt fehl, FK unvollständig. (Cockpit-DB hat aktuell keine — generisch trotzdem falsch.)
- **Fix:** FK-Spalten in `read_schema()` pro Constraint-Name gruppieren (Liste `from_cols`/`to_cols`), DDL mit Spaltenlisten erzeugen.

### 2.10 Generierte Hilfsspalten (`_UX_*_key`) stören Schema-Diff und Rename-Erkennung
- **Wo:** `src/schema_diff.py` → `read_mysql_schema()` liest alle Spalten, auch `GENERATED`-Spalten aus Issue #33.
- **Symptom:** Nach dem ersten Deploy eines emulierten gefilterten Index erscheint `_UX_…_key` bei jedem Diff als „Spalte existiert in MySQL aber nicht mehr in MDF" — Dauerwarnung. Schlimmer: `detect_rename_candidates()` könnte die VARCHAR(255)-Hilfsspalte als Umbenennungs-Kandidat für eine neue echte Spalte vorschlagen (inkl. DROP!).
- **Fix:** In `read_mysql_schema()` das bereits gelesene `EXTRA`-Feld auswerten: Spalten mit `GENERATED` markieren und aus `removed_columns` + Rename-Kandidaten ausschließen.

### 2.11 Schema-Diff legt neue Tabellen ohne Indexe und FKs an
- **Wo:** `src/schema_diff.py` → `generate_diff_ddl()`: `new_tables` erzeugt nur Spalten + PK; `new_indexes`/`new_fks` existieren nur für `altered_tables`.
- **Symptom:** Kommt eine neue Tabelle per Schema-Diff dazu (wie `TablePlatingRate`), fehlen ihre UNIQUE-Indexe und Foreign Keys — genau so ist der Live-Zustand von `TablePlatingRate` entstanden (nur PRIMARY KEY vorhanden, Issue #33).
- **Fix:** Für jede neue Tabelle nach dem `CREATE TABLE` auch `render_index_ddl()` und die FK-Constraints ausgeben.

---

## P3 — Mittel (Robustheit und Bedienbarkeit)

### 3.1 Runner nehmen immer das erste Profil
- **Wo:** `run_headless.py` / `run_migrate_data.py`: `profile = next(iter(all_cfg))`.
- **Symptom:** `config.json` enthält drei Profile (Standard, Cockpit, ProductionTable) — headless läuft immer „Standard", ohne Auswahlmöglichkeit.
- **Fix:** `--profile <Name>`-Argument (argparse), Default = erstes Profil, unbekannter Name = Fehlermeldung mit Liste.

### 3.2 `run_migrate_data.py` liefert Exit-Code 0 trotz Fehlern
- **Wo:** Zusammenfassung am Ende: Fehler werden geloggt, aber `sys.exit(1)` fehlt.
- **Symptom:** Skripte/Automatisierung können einen fehlgeschlagenen Lauf nicht erkennen.
- **Fix:** `sys.exit(1 if result["errors"] or result["cancelled"] else 0)`.

### 3.3 GUI-Logging aus Worker-Threads ist nicht thread-sicher
- **Wo:** `src/ui.py` → `log()` schreibt direkt ins Tk-Text-Widget, wird aber aus `threading.Thread`-Tasks aufgerufen (`_read_schema`, `_deploy`, …).
- **Symptom:** Tkinter ist nicht thread-sicher; das funktioniert „meistens", kann aber sporadisch einfrieren/abstürzen — typisch: nicht reproduzierbare UI-Hänger.
- **Fix:** Log-Zeilen in eine `queue.Queue` schreiben; Main-Thread pollt per `self.after(100, …)` und schreibt ins Widget. (`_ask_yes_no_threadsafe` macht es bereits richtig vor.)

### 3.4 Berechnete Spalten (computed columns) würden als normale Spalten migriert
- **Wo:** `src/mssql.py` → `read_schema()` (INFORMATION_SCHEMA.COLUMNS enthält computed columns ohne Kennzeichnung).
- **Symptom:** Eine MSSQL-computed-column würde in MySQL als normale Spalte angelegt und mit den berechneten Werten statisch befüllt — Formel geht verloren, Werte veralten.
- **Fix:** `COLUMNPROPERTY(…, 'IsComputed')` mitlesen; erkannte Spalten als Warnung flaggen (oder als `GENERATED ALWAYS AS` übersetzen, wenn die Formel einfach ist).

### 3.5 Kleinere Punkte
- `src/migrate_data.py` → `migrate_table()`: Cursor wird bei früher Rückkehr (Abbruch/leer) nicht geschlossen; `get_row_counts()` schlüsselt nur nach Tabellennamen (Kollision bei gleichem Namen in zwei Schemata).
- `src/mssql.py`: `sp_detach_db`/`ALTER DATABASE` bauen SQL per f-String — ein `'` oder `]` im DB-Namen bricht das Statement (lokales Tool, geringes Risiko; `db_attach_name` wird in der UI bereits auf `[a-zA-Z0-9_]` gefiltert, im Headless-Pfad nicht).
- `src/ui.py`: `_generate_ddl()` und `_test_mysql()` laufen im Main-Thread — bei großen Schemata bzw. nicht erreichbarem Server friert die UI bis zu 5 s ein.
- `src/migrate_data.py`: `started_at` aus dem Checkpoint wird geladen, aber nie angezeigt/verwendet.

---

## P4 — Dokumentation (Abgleich Code ↔ Doku)

### 4.1 `README.md` ist veraltet — *(mit diesem Review aktualisiert)*
Fehlten: `schema_diff.py`, `migrate_data.py`, `audit_triggers.py`, `run_migrate_data.py`, `tests/`, `documentation/` in Struktur & Abhängigkeitsdiagramm; Features Schema-Diff, Datenmigration (Chunks/Checkpoint/Resume/Dry-Run), Rename-Erkennung, Audit-Trigger, WCEP-Spiegelung.

### 4.2 `MSSQL_MySQL_Uebersetzungsplan.pdf` enthält falsche/veraltete Aussagen
- **T1b** „TINYINT → TINYINT UNSIGNED fehlt" → **längst umgesetzt** (Issue #17/PR #18).
- **P4** „Audit-Trigger fehlen" → **umgesetzt** (`src/audit_triggers.py`, PR #20/#24).
- **X2** „Trigger werden unverarbeitet ins DDL geschrieben" → **stimmt nicht**: `read_schema()` liest gar keine Trigger; es landet nichts Ungültiges im DDL. (Der sinnvolle Kern — LastChange-Trigger als `ON UPDATE CURRENT_TIMESTAMP` nachbilden — bleibt als Idee gültig.)
- **T5b** „`N'…'` führt in MySQL zu Syntaxfehlern" → **fachlich falsch**: MySQL und MariaDB unterstützen `N'literal'` nativ (Introducer für utf8). Kein Handlungsbedarf im Code.
- **Fix:** Statustabelle in `generate_translation_plan_pdf.py` korrigieren und PDF neu generieren — oder das PDF durch dieses TODO.md als lebende Quelle ersetzen.

### 4.3 `AUDIT_TRIGGERS_REVIEW.md` las sich wie eine offene Punch-Liste — *(mit diesem Review aktualisiert)*
Alle Punkte P1–P5 sind seit PR #24 umgesetzt; ein Status-Kopf stellt das jetzt klar. Offen bleibt nur der Hinweis aus P3, dass der WCEP-Dump (`Cockpit_DatenBank.sql`) `TableAuditLog` noch als `Id INT` + `LONGTEXT` definiert statt `BIGINT` + `JSON` — das gehört ins WCEP-Repo.

### 4.4 PDF-Generator-Skripte liegen im falschen Kontext
`generate_steuerung_pdf.py` / `generate_translation_plan_pdf.py` erzeugen Doku-PDFs, haben aber eine ungenannte Abhängigkeit (`reportlab`), die nicht in `install_deps.bat` steht. Mindestens im README/Skript-Kopf dokumentieren.

---

## Bewusst NICHT auf der Liste

- `N'…'`-Literale übersetzen — unnötig, MySQL versteht sie (siehe 4.2).
- `tablelanguage`/`tablelanguagecustom` in MySQL — bewusst von WCEP gepflegt, kein Migrationsgegenstand.
- `DELIMITER`-Syntax in `audit_triggers_*.sql` — gewollt (manuelle Ausführung per mysql-CLI), im Tool-Log dokumentiert.

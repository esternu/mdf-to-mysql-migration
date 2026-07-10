# Review: `audit_triggers_<db>.sql` — Punch-Liste für den Generator

> **Status: ✅ ERLEDIGT** — alle Punkte P1–P5 wurden mit
> [Issue #23 / PR #24](https://github.com/esternu/mdf-to-mysql-migration/pull/24)
> in `src/audit_triggers.py` umgesetzt (P1 JSON-Pfad-Quoting, P2 BLOB-Ausschluss,
> P3 `CREATE TABLE IF NOT EXISTS`, P4 lokale Trigger-Variablen, P5 PK-Änderungs-Logging).
> Dieses Dokument bleibt als Begründung der Design-Entscheidungen erhalten.
>
> **Noch offen (anderes Repo):** Der zweite Teil von P3 — der kanonische
> WCEP-Dump `WCEP/Tools/schema/Cockpit_DatenBank.sql` definiert `TableAuditLog`
> weiterhin als `Id INT` + `LONGTEXT` statt `Id BIGINT` + `JSON`. Angleichung
> gehört ins WCEP-Repo.

Bewertung der von `mdf-to-mysql-migration` erzeugten Datei
`Tools/schema/audit_triggers_Cockpit_Datenbank.sql` (Stand: 13 Tabellen ×
INSERT/UPDATE/DELETE, JSON-Payload, Delta-Logging beim UPDATE).

**Verdikt (ursprünglich):** tragfähiger Ansatz, aber **vor Produktiveinsatz müssen P1–P3
behoben werden** — sonst schlagen UPDATEs auf ~der Hälfte der Tabellen fehl und
Bild-Tabellen brechen. Alle Fixes gehören in den **Generator**, nicht in die
generierte `.sql`.

Zieldatenbank: **MariaDB 10.11** (Synology).

---

## P1 🔴 UPDATE-Trigger: JSON-Pfade mit Sonderzeichen müssen gequotet werden
**Symptom:** `JSON_SET(@audit_old, '$.Metal per gramm', …)` → *„Invalid JSON path
expression"* → der Trigger wirft → das **UPDATE der App schlägt fehl**.
Betroffen: jede Spalte mit Leerzeichen/Klammer/Bindestrich (TableCost,
TablePlating, TableJig, TableProcessAir/Water, TableSolution). INSERT/DELETE
sind NICHT betroffen (dort ist der Name ein `JSON_OBJECT`-Key = reiner String).

```sql
-- vorher (kaputt):
JSON_SET(@audit_old, '$.Metal per gramm', OLD.`Metal per gramm`)
-- nachher (korrekt): Spaltenname im Pfad in DOUBLE QUOTES
JSON_SET(@audit_old, '$."Metal per gramm"', OLD.`Metal per gramm`)
```
**Generator:** Pfad als `'$."' + spalte + '"'` bauen. Sicherheitshalber `"` im
Spaltennamen escapen (`"` → `\"`); die WCEP-Spalten haben keine, aber robust ist
besser.

## P2 🔴 BLOB-Spalten gehören nicht ins Audit-JSON
**Symptom:** `Picture` / `Jig Picture` (LONGBLOB) werden in `JSON_OBJECT`
geschrieben (TableArticle, TableJig, TableSolution). In MySQL harter Fehler
(*binary charset*); in MariaDB unsauberes/riesiges JSON. Selbst wenn es lädt:
jedes INSERT/DELETE kopiert das **ganze Bild** (oft MB) in `TableAuditLog`.

**Generator:** Spalten vom Typ `*blob`/`varbinary`/`binary` aus dem Audit-JSON
**ausschließen** — sowohl aus `JSON_OBJECT` (INSERT/DELETE) als auch aus den
per-Spalte-`IF`-Blöcken (UPDATE). Optional Marker statt Inhalt:
```sql
'Picture_bytes', OCTET_LENGTH(NEW.`Picture`)
```

## P3 🔴 Audit-Tabelle nicht destruktiv anlegen + mit Dump angleichen
**Symptom:** `DROP TABLE IF EXISTS TableAuditLog` am Dateianfang **löscht die
komplette Historie** bei jedem Re-Run. Zudem widerspricht die Definition hier
(`Id BIGINT`, `OldValue/NewValue JSON`) dem kanonischen Dump
(`Cockpit_DatenBank.sql`: `Id INT`, `LONGTEXT`) → zwei divergierende
Definitionen derselben Tabelle.

**Generator:**
- `CREATE TABLE IF NOT EXISTS` statt `DROP`+`CREATE` (nie die Audit-Tabelle
  droppen).
- **Eine** kanonische Definition: entweder den Dump auf `Id BIGINT` + `JSON`
  heben (empfohlen, ist die bessere Variante) oder hier auf `INT`/`LONGTEXT`
  zurück — aber konsistent zum Dump. Idealerweise definiert der Dump die Tabelle,
  und das Trigger-Skript legt nur die Trigger an.

---

## P4 🟡 Lokale Variablen statt Session-Variablen
`@audit_old/@audit_new/@audit_changed` sind verbindungsweit und können bei
verschachtelten Triggern kollidieren / lecken in die Session.

```sql
BEGIN
  DECLARE v_old JSON DEFAULT JSON_OBJECT('PlatingId', OLD.`PlatingId`);
  DECLARE v_new JSON DEFAULT JSON_OBJECT('PlatingId', NEW.`PlatingId`);
  DECLARE v_changed INT DEFAULT 0;
  ...
END
```
(`DECLARE` muss am Block-Anfang stehen.)

## P5 🟡 Reine PK-Änderung wird nicht geloggt
Bei `TableProduct` (composite PK) prüft der UPDATE-Trigger nur Nicht-PK-Spalten;
ändert man nur `Layer`, bleibt `changed=0` → kein Eintrag. Falls PK-Änderungen
auditiert werden sollen: PK-Spalten ebenfalls per `<=>` vergleichen und ggf.
`changed=1` setzen.

---

## Was gut ist (beibehalten)
- Pro Tabelle `AFTER INSERT/UPDATE/DELETE`, JSON-Payload.
- UPDATE = nur geänderte Felder (Delta) + PK als Kontext; Skip bei No-op-Update.
- `DROP TRIGGER IF EXISTS` je Trigger → re-runnable.
- `TableAuditLog`, `row_locks` werden korrekt nicht auditiert.
- `ChangedBy = NEW/OLD.Editor` passt zur App (setzt `Editor` aus `X-User-Name`).

## Test-Empfehlung
Nach den Fixes: Schema + Trigger in `mariadb:10.11` laden, dann je ein
INSERT/UPDATE/DELETE auf einer Tabelle **mit** Sonderzeichen-Spalten (z.B.
`TableCost`) und einer **mit** BLOB (`TableJig`) ausführen und prüfen, dass genau
eine `TableAuditLog`-Zeile mit korrektem `Action`/`ChangedBy`/`OldValue`/
`NewValue` entsteht und kein Bild-Inhalt im JSON landet.

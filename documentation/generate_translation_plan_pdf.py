"""
Implementierungsplan: MSSQL → MySQL Übersetzungs-Lücken
========================================================
Generiert MSSQL_MySQL_Uebersetzungsplan.pdf im WCEP-Stil.

Ausführen:
    py documentation/generate_translation_plan_pdf.py
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import PageTemplate, Frame
from reportlab.platypus.doctemplate import BaseDocTemplate
import os

# ── Farben ────────────────────────────────────────────────────────────────────
NAVY   = colors.HexColor("#0d1b3e")
GOLD   = colors.HexColor("#c8a84b")
GREEN  = colors.HexColor("#1a7a3c")
RED    = colors.HexColor("#b22222")
AMBER  = colors.HexColor("#b36200")
BLUE   = colors.HexColor("#1a3a6b")
LGRAY  = colors.HexColor("#f4f4f4")
MGRAY  = colors.HexColor("#cccccc")
WHITE  = colors.white
BLACK  = colors.black

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm

# ── Output-Pfad ───────────────────────────────────────────────────────────────
_HERE    = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(_HERE, "MSSQL_MySQL_Uebersetzungsplan.pdf")


# ════════════════════════════════════════════════════════════════════════════
#  Styles
# ════════════════════════════════════════════════════════════════════════════
def make_styles():
    base = getSampleStyleSheet()

    def ps(name, **kw):
        return ParagraphStyle(name, parent=base["Normal"], **kw)

    return {
        "title":      ps("title",   fontName="Helvetica-Bold",  fontSize=22,
                         textColor=WHITE,   spaceAfter=4,  leading=26),
        "subtitle":   ps("subtitle",fontName="Helvetica",       fontSize=12,
                         textColor=GOLD,    spaceAfter=2,  leading=15),
        "h1":         ps("h1",      fontName="Helvetica-Bold",  fontSize=14,
                         textColor=NAVY,    spaceBefore=10, spaceAfter=4,  leading=18),
        "h2":         ps("h2",      fontName="Helvetica-Bold",  fontSize=11,
                         textColor=BLUE,    spaceBefore=8,  spaceAfter=3,  leading=14),
        "body":       ps("body",    fontName="Helvetica",       fontSize=9,
                         textColor=BLACK,   spaceAfter=3,  leading=13),
        "small":      ps("small",   fontName="Helvetica",       fontSize=8,
                         textColor=colors.HexColor("#444444"), leading=11),
        "code":       ps("code",    fontName="Courier",         fontSize=8,
                         textColor=NAVY,    leading=11,    backColor=LGRAY,
                         borderPadding=3),
        "ok":         ps("ok",      fontName="Helvetica-Bold",  fontSize=9,
                         textColor=GREEN),
        "miss":       ps("miss",    fontName="Helvetica-Bold",  fontSize=9,
                         textColor=RED),
        "warn":       ps("warn",    fontName="Helvetica-Bold",  fontSize=9,
                         textColor=AMBER),
        "tbl_head":   ps("tbl_head",fontName="Helvetica-Bold",  fontSize=8,
                         textColor=WHITE),
        "tbl_cell":   ps("tbl_cell",fontName="Helvetica",       fontSize=8,
                         textColor=BLACK,   leading=11),
        "tbl_code":   ps("tbl_code",fontName="Courier",         fontSize=7.5,
                         textColor=NAVY,    leading=11),
        "footer":     ps("footer",  fontName="Helvetica",       fontSize=7,
                         textColor=colors.HexColor("#888888"), alignment=TA_CENTER),
    }


# ════════════════════════════════════════════════════════════════════════════
#  Seiten-Callback (Kopf- und Fußzeile)
# ════════════════════════════════════════════════════════════════════════════
def on_page(canvas, doc):
    canvas.saveState()
    w, h = A4

    # Goldene Kopfzeile (außer Titelseite)
    if doc.page > 1:
        canvas.setFillColor(NAVY)
        canvas.rect(0, h - 14 * mm, w, 14 * mm, fill=1, stroke=0)
        canvas.setFillColor(GOLD)
        canvas.rect(0, h - 15.5 * mm, w, 1.5 * mm, fill=1, stroke=0)
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(WHITE)
        canvas.drawString(MARGIN, h - 10 * mm, "MDF → MySQL Migrations-Tool")
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(w - MARGIN, h - 10 * mm,
                               "MSSQL→MySQL Übersetzungsplan")

    # Fußzeile
    canvas.setFillColor(NAVY)
    canvas.rect(0, 0, w, 10 * mm, fill=1, stroke=0)
    canvas.setFillColor(GOLD)
    canvas.rect(0, 10 * mm, w, 1 * mm, fill=1, stroke=0)
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(WHITE)
    canvas.drawString(MARGIN, 3.5 * mm, "Projekt: mdf-to-mysql-migration")
    canvas.drawCentredString(w / 2, 3.5 * mm, "Stand: 2026-06-09")
    canvas.drawRightString(w - MARGIN, 3.5 * mm, f"Seite {doc.page}")
    canvas.restoreState()


# ════════════════════════════════════════════════════════════════════════════
#  Hilfs-Flowables
# ════════════════════════════════════════════════════════════════════════════
def section_header(title, s):
    return [
        HRFlowable(width="100%", thickness=2, color=GOLD, spaceAfter=4),
        Paragraph(title, s["h1"]),
    ]


def subsection(title, s):
    return Paragraph(title, s["h2"])


def body(text, s):
    return Paragraph(text, s["body"])


def code_block(text, s):
    lines = text.strip().split("\n")
    items = [Paragraph(ln.replace(" ", "&nbsp;"), s["code"]) for ln in lines]
    data  = [[item] for item in items]
    t = Table(data, colWidths=[PAGE_W - 2 * MARGIN])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, -1), LGRAY),
        ("BOX",         (0, 0), (-1, -1), 0.5, MGRAY),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",(0, 0), (-1, -1), 6),
        ("TOPPADDING",  (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING",(0,0), (-1, -1), 2),
    ]))
    return t


def status_badge(text, color):
    return f'<font color="{color.hexval()}">{text}</font>'


def info_box(text, s, bg=colors.HexColor("#e8f0fb"), border=BLUE):
    data = [[Paragraph(text, s["body"])]]
    t = Table(data, colWidths=[PAGE_W - 2 * MARGIN])
    t.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, -1), bg),
        ("BOX",          (0, 0), (-1, -1), 1.0, border),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
    ]))
    return t


def warn_box(text, s):
    return info_box(text, s,
                    bg=colors.HexColor("#fff8e1"),
                    border=AMBER)


def ok_box(text, s):
    return info_box(text, s,
                    bg=colors.HexColor("#e8f5e9"),
                    border=GREEN)


def grid_table(headers, rows, s, col_widths=None):
    """Generische Tabelle mit navy Header-Zeile."""
    usable = PAGE_W - 2 * MARGIN
    if col_widths is None:
        col_widths = [usable / len(headers)] * len(headers)

    head_row = [Paragraph(h, s["tbl_head"]) for h in headers]
    data     = [head_row]
    for row in rows:
        data.append([Paragraph(str(c), s["tbl_cell"]) for c in row])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    n = len(data)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("BACKGROUND",    (0, 1), (-1, -1), WHITE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [WHITE, LGRAY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, MGRAY),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ]))
    return t


# ════════════════════════════════════════════════════════════════════════════
#  Titelseite
# ════════════════════════════════════════════════════════════════════════════
def build_cover(s, story):
    from reportlab.platypus import Spacer

    # Navy-Hintergrund simulieren via Tabelle
    cover_data = [[
        Paragraph("MDF → MySQL Migration Tool", s["title"]),
        Spacer(1, 4 * mm),
        Paragraph("MSSQL → MySQL Übersetzungsplan", s["subtitle"]),
        Spacer(1, 2 * mm),
        Paragraph("Implementierungslücken · Priorisierung · Umsetzungsplanung",
                  s["subtitle"]),
    ]]
    cover_table = Table([[item] for item in cover_data[0]],
                        colWidths=[PAGE_W - 2 * MARGIN])
    cover_table.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, -1), NAVY),
        ("LEFTPADDING",  (0, 0), (-1, -1), 16),
        ("RIGHTPADDING", (0, 0), (-1, -1), 16),
        ("TOPPADDING",   (0, 0), (0, 0),   30),
        ("BOTTOMPADDING",(0, -1),(-1, -1), 20),
    ]))
    story.append(cover_table)
    story.append(Spacer(1, 6 * mm))

    # Gold-Trennlinie
    story.append(HRFlowable(width="100%", thickness=3, color=GOLD))
    story.append(Spacer(1, 4 * mm))

    toc_rows = [
        ["1", "Statusübersicht", "Welche Punkte sind bereits implementiert?"],
        ["2", "Fehlende Features", "Detail-Analyse der 5 offenen Lücken"],
        ["3", "Implementierungsplan", "Reihenfolge, Aufwand, Datei-Zuordnung"],
        ["4", "Akzeptanzkriterien", "Wie wird Erfolg gemessen?"],
    ]
    story.append(grid_table(
        ["#", "Abschnitt", "Inhalt"],
        toc_rows, s,
        col_widths=[12 * mm, 50 * mm, PAGE_W - 2 * MARGIN - 62 * mm],
    ))
    story.append(Spacer(1, 4 * mm))
    story.append(info_box(
        "<b>Kontext:</b> Dieses Dokument beschreibt die noch fehlenden Übersetzungs-Regeln "
        "im Tool <i>mdf-to-mysql-migration</i>, die für eine saubere Migration der "
        "<i>Cockpit_Databases</i>-MDF nach MySQL/MariaDB erforderlich sind. "
        "Grundlage: MySQL_Migration_ToDo.md + MySQL_Portability_Report.md "
        "(Projekt Cockpit_ElectroPlating_2.0_Database).", s))


# ════════════════════════════════════════════════════════════════════════════
#  Abschnitt 1 – Statusübersicht
# ════════════════════════════════════════════════════════════════════════════
def build_section1(s, story):
    story += section_header("1 · Statusübersicht", s)
    story.append(body(
        "Stand: Branch <b>master</b> · geprüfte Dateien: "
        "<b>src/transform.py</b>, <b>src/deploy.py</b>, <b>src/mssql.py</b>.", s))
    story.append(Spacer(1, 3 * mm))

    rows = [
        # P1 Tabellen
        ["T1a", "Typ-Mapping (alle außer TINYINT)", "P1",    "✅ implementiert"],
        ["T1b", "TINYINT → TINYINT UNSIGNED",        "P1",    "❌ fehlt"],
        ["T2",  "IDENTITY → AUTO_INCREMENT",          "P1",    "✅ implementiert"],
        ["T3",  "Defaults (GETDATE → CURRENT_TS)",    "P1",    "✅ implementiert"],
        ["T4",  "Storage-Optionen strippen",           "P1",    "✅ implementiert"],
        ["T5a", "[name] → Backtick",                   "P1",    "✅ implementiert"],
        ["T5b", "N'...' Unicode-Literale in Views",    "P1",    "❌ fehlt"],
        ["T5c", "GO / SET NOCOUNT ON",                 "P1",    "n/a (MDF-Workflow)"],
        # P2 Views
        ["V1",  "STRING_AGG → GROUP_CONCAT",           "P2",    "✅ implementiert"],
        ["V2",  "ISNULL → IFNULL",                     "P2",    "✅ implementiert"],
        ["V3",  "CAST AS MONEY → DECIMAL",             "P2",    "✅ implementiert"],
        ["V4",  "OUTER/CROSS APPLY → LEFT JOIN",       "P2",    "✅ implementiert"],
        ["V5",  "CTE Versions-Hinweis",                "P2",    "❌ fehlt"],
        # P3 Prozedurale Logik
        ["X1",  "MERGE → Flagging",                    "P3",    "❌ fehlt"],
        ["X2",  "Trigger → Flagging + Hinweise",       "P3",    "❌ fehlt"],
        ["X3",  "OPENROWSET → Flagging",               "P3",    "n/a (MDF-Workflow)"],
        ["X4",  "SQLCMD-Spezifika",                    "P3",    "n/a (MDF-Workflow)"],
        # P4
        ["P4",  "Engine-spez. Objekte → Flagging",     "P4",    "❌ fehlt"],
    ]

    # Farbe nach Status
    styled_rows = []
    for r in rows:
        status_text = r[3]
        if "✅" in status_text:
            status = Paragraph(status_text, ParagraphStyle("s_ok",
                parent=s["tbl_cell"], textColor=GREEN))
        elif "❌" in status_text:
            status = Paragraph(status_text, ParagraphStyle("s_miss",
                parent=s["tbl_cell"], textColor=RED))
        else:
            status = Paragraph(status_text, ParagraphStyle("s_na",
                parent=s["tbl_cell"], textColor=AMBER))
        styled_rows.append([r[0], r[1], r[2], status])

    usable = PAGE_W - 2 * MARGIN
    story.append(grid_table(
        ["#", "Feature", "Prio", "Status"],
        [],  # we'll build manually
        s,
        col_widths=[12 * mm, usable - 12 - 14 - 40*mm, 14 * mm, 40 * mm],
    ))
    # Override with styled version
    story.pop()  # remove empty table

    head_row = [Paragraph(h, s["tbl_head"]) for h in ["#", "Feature", "Prio", "Status"]]
    data = [head_row]
    for r in styled_rows:
        data.append([
            Paragraph(r[0], s["tbl_cell"]),
            Paragraph(r[1], s["tbl_cell"]),
            Paragraph(r[2], s["tbl_cell"]),
            r[3],
        ])

    col_widths = [12 * mm, usable - 12*mm - 14*mm - 42*mm, 14 * mm, 42 * mm]
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [WHITE, LGRAY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, MGRAY),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(t)
    story.append(Spacer(1, 3 * mm))
    story.append(ok_box(
        "<b>Ergebnis P1/P2:</b> Tabellen-Layer und View-Konvertierung sind zu "
        "<b>~85 %</b> implementiert. Die 5 offenen Punkte (T1b, T5b, V5, X2, P4) "
        "blockieren einzelne Objekte, nicht die gesamte Migration.", s))


# ════════════════════════════════════════════════════════════════════════════
#  Abschnitt 2 – Fehlende Features (Detail)
# ════════════════════════════════════════════════════════════════════════════
def build_section2(s, story):
    story += section_header("2 · Fehlende Features – Detail-Analyse", s)

    # ── T1b: TINYINT ─────────────────────────────────────────────────────────
    story.append(KeepTogether([
        subsection("T1b · TINYINT → TINYINT UNSIGNED", s),
        body("<b>Problem:</b> SQL Server's TINYINT hat den Wertebereich 0–255 (unsigned). "
             "MySQL's TINYINT ist standardmäßig signed (–128 bis 127). "
             "Werte > 127 aus der MDF würden beim INSERT in MySQL überlaufen.", s),
        Spacer(1, 2 * mm),
    ]))
    story.append(grid_table(
        ["Vorher (MDF)", "Nachher (MySQL)"],
        [
            ["`[Layer] TINYINT NOT NULL`",
             "`` `Layer` TINYINT UNSIGNED NOT NULL ``"],
        ], s,
        col_widths=[(PAGE_W - 2 * MARGIN) / 2] * 2,
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(info_box(
        "<b>Betroffene Datei:</b> src/transform.py — TYPE_MAP<br/>"
        "<b>Fix:</b> `\"tinyint\": \"TINYINT UNSIGNED\"` in TYPE_MAP ändern.<br/>"
        "<b>Aufwand:</b> 1 Zeile + 1 Test.", s))
    story.append(Spacer(1, 4 * mm))

    # ── T5b: N'...' ───────────────────────────────────────────────────────────
    story.append(KeepTogether([
        subsection("T5b · N'...' Unicode-Literale in View-Körpern", s),
        body("<b>Problem:</b> SQL Server verwendet `N'text'` für Unicode-String-Literale. "
             "In MySQL sind alle Strings standardmäßig Unicode (UTF-8) – "
             "das `N`-Präfix wird nicht unterstützt und führt zu Syntax-Fehlern.", s),
        Spacer(1, 2 * mm),
    ]))
    story.append(grid_table(
        ["Vorher (T-SQL in View)", "Nachher (MySQL)"],
        [
            ["WHERE Name = N'Test'", "WHERE Name = 'Test'"],
            ["CONVERT(NVARCHAR, N'abc')", "CONVERT(CHAR, 'abc')"],
        ], s,
        col_widths=[(PAGE_W - 2 * MARGIN) / 2] * 2,
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(info_box(
        "<b>Betroffene Datei:</b> src/transform.py — convert_view_sql()<br/>"
        "<b>Fix:</b> Regex `r\"\\bN'\"` → `\"'\"` nach den bestehenden Typ-Ersetzungen.<br/>"
        "<b>Aufwand:</b> 1 Zeile + 1 Test.", s))
    story.append(Spacer(1, 4 * mm))

    # ── V5: CTE-Hinweis ───────────────────────────────────────────────────────
    story.append(KeepTogether([
        subsection("V5 · CTE Versions-Hinweis", s),
        body("<b>Problem:</b> Mehrere Views verwenden CTEs (`WITH ... AS (...)`). "
             "Diese sind nur ab MySQL 8.0 / MariaDB 10.2 unterstützt. "
             "Aktuell erscheint kein Hinweis im generierten DDL.", s),
        Spacer(1, 2 * mm),
    ]))
    story.append(grid_table(
        ["Gewünschter Output im DDL"],
        [["-- ⚠ CTE verwendet: erfordert MySQL >= 8.0 / MariaDB >= 10.2"],
         ["WITH LastValues AS (SELECT ...)"]],
        s,
        col_widths=[PAGE_W - 2 * MARGIN],
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(info_box(
        "<b>Betroffene Datei:</b> src/transform.py — convert_view_sql()<br/>"
        "<b>Fix:</b> Nach Header-Entfernung: `if re.search(r'\\bWITH\\b.*\\bAS\\b.*\\(',sql,re.IGNORECASE|re.DOTALL):`"
        " → `warnings.append('CTE verwendet: ...')`<br/>"
        "<b>Aufwand:</b> 3 Zeilen + 1 Test.", s))
    story.append(Spacer(1, 4 * mm))

    # ── X2: Trigger-Flagging ─────────────────────────────────────────────────
    story.append(KeepTogether([
        subsection("X2 · Trigger-Erkennung & MANUELL-Flagging", s),
        body("<b>Problem:</b> SQL Server-Trigger sind set-basiert (`inserted`/`deleted`-Pseudotabellen) "
             "und können nicht 1:1 in MySQL's row-basierte Trigger übersetzt werden. "
             "Aktuell werden Trigger aus der MDF gelesen und unverarbeitet ins DDL geschrieben "
             "– dies führt zu Syntax-Fehlern auf MySQL.", s),
        Spacer(1, 2 * mm),
    ]))
    story.append(warn_box(
        "⚠ Betroffen: <b>alle 12 LastChange-Trigger</b> + Audit-Trigger (TriggerTableAudit). "
        "Diese erscheinen im generierten DDL als ungültige T-SQL-Blöcke.", s))
    story.append(Spacer(1, 2 * mm))
    story.append(grid_table(
        ["Was das Tool tun soll", "Ausgabe im DDL"],
        [
            ["Trigger-DDL als MANUELL flaggen",
             "-- ⚠ MANUELL: CREATE TRIGGER trg_TableX_LastChange\n"
             "-- Empfehlung: Spalte LastChange DATETIME\n"
             "--   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n"
             "-- (Trigger-Body wird NICHT übersetzt)"],
            ["Mapping-Hinweis für Audit-Trigger",
             "-- inserted → NEW  |  deleted → OLD\n"
             "-- IF UPDATE(col) → NEW.col <=> OLD.col\n"
             "-- Gerüst: CREATE TRIGGER ... FOR EACH ROW BEGIN...END"],
        ], s,
        col_widths=[(PAGE_W - 2 * MARGIN) * 0.35,
                    (PAGE_W - 2 * MARGIN) * 0.65],
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(info_box(
        "<b>Betroffene Datei:</b> src/transform.py — generate_mysql_ddl() + neuer Block<br/>"
        "<b>MDF-Lese-Seite:</b> src/mssql.py — Trigger aus sys.triggers lesen<br/>"
        "<b>Aufwand:</b> ~2–3 Stunden (Lesen + Flagging + Hinweis-Generator + Tests).", s))
    story.append(Spacer(1, 4 * mm))

    # ── P4: Engine-spez. Flagging ─────────────────────────────────────────────
    story.append(KeepTogether([
        subsection("P4 · Engine-spezifische Objekte – Flagging-Mechanismus", s),
        body("<b>Problem:</b> LastChange-Trigger, Audit-Generator und ViewAuditChanges "
             "sind komplett engine-spezifisch und müssen für MySQL <b>neu geschrieben</b> "
             "werden. Das Tool sollte diese Objekte <b>identifizieren</b> und "
             "einen separaten Report ausgeben – statt sie still zu überspringen.", s),
        Spacer(1, 2 * mm),
    ]))
    story.append(grid_table(
        ["Objekt-Typ", "Anzahl", "Empfehlung"],
        [
            ["LastChange-Trigger (trg_*_LastChange)", "12",
             "Ersetzen durch ON UPDATE CURRENT_TIMESTAMP-Spalte"],
            ["Audit-Trigger (TriggerTableAudit)", "1",
             "Neu schreiben: FOR EACH ROW + JSON_OBJECT(...)"],
            ["ViewAuditChanges", "1",
             "Entfällt – row-Trigger liefert direkt nur Änderungen"],
        ], s,
        col_widths=[(PAGE_W - 2 * MARGIN) * 0.45,
                    (PAGE_W - 2 * MARGIN) * 0.10,
                    (PAGE_W - 2 * MARGIN) * 0.45],
    ))
    story.append(Spacer(1, 2 * mm))
    story.append(info_box(
        "<b>Betroffene Datei:</b> src/transform.py — generate_mysql_ddl(), neuer Abschnitt<br/>"
        "<b>Output:</b> Liste 'MANUELL ZU PRUEFEN' am Ende des generierten DDL<br/>"
        "<b>Aufwand:</b> ~1 Stunde (Erkennung + Report-Block).", s))


# ════════════════════════════════════════════════════════════════════════════
#  Abschnitt 3 – Implementierungsplan
# ════════════════════════════════════════════════════════════════════════════
def build_section3(s, story):
    story += section_header("3 · Implementierungsplan", s)
    story.append(body(
        "Alle 5 offenen Punkte sind unabhängig voneinander umsetzbar. "
        "Empfohlene Reihenfolge nach Aufwand/Nutzen-Verhältnis:", s))
    story.append(Spacer(1, 3 * mm))

    rows = [
        ["1", "T1b",
         "TINYINT UNSIGNED",
         "src/transform.py\nTYPE_MAP",
         "1 Zeile\n+ 1 Test",
         "⚡ Sofort"],
        ["2", "T5b",
         "N'...' entfernen",
         "src/transform.py\nconvert_view_sql()",
         "1 Zeile\n+ 1 Test",
         "⚡ Sofort"],
        ["3", "V5",
         "CTE-Hinweis",
         "src/transform.py\nconvert_view_sql()",
         "3 Zeilen\n+ 1 Test",
         "⚡ Sofort"],
        ["4", "X2",
         "Trigger-Flagging\n+ Hinweise",
         "src/mssql.py\nsrc/transform.py",
         "2–3 h\n+ Tests",
         "🔶 Issue #16"],
        ["5", "P4",
         "Engine-spez.\nFlagging-Report",
         "src/transform.py\ngenerate_mysql_ddl()",
         "~1 h\n+ Tests",
         "🔶 Issue #17"],
    ]

    usable = PAGE_W - 2 * MARGIN
    col_w = [10*mm, 12*mm, usable - 10 - 12 - 55 - 28 - 28*mm, 55*mm, 28*mm, 28*mm]

    head_row = [Paragraph(h, s["tbl_head"])
                for h in ["#", "ID", "Beschreibung", "Datei(en)", "Aufwand", "Status"]]
    data = [head_row]
    for r in rows:
        data.append([Paragraph(c.replace("\n", "<br/>"), s["tbl_cell"]) for c in r])

    t = Table(data, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [WHITE, LGRAY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, MGRAY),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        # Quick wins grün markieren
        ("BACKGROUND",    (0, 1), (-1, 3),  colors.HexColor("#e8f5e9")),
    ]))
    story.append(t)
    story.append(Spacer(1, 3 * mm))

    story.append(ok_box(
        "<b>Quick Wins (Punkte 1–3):</b> T1b, T5b und V5 können in einem einzigen "
        "kleinen Commit umgesetzt werden – insgesamt unter 10 Zeilen Code, "
        "sofortiger Nutzen für alle View-Migrationen.", s))
    story.append(Spacer(1, 3 * mm))
    story.append(warn_box(
        "<b>Punkte 4–5 (Trigger/Engine-Flagging)</b> erfordern zunächst das Lesen von "
        "Trigger-DDL aus der MDF (sys.triggers + sys.sql_modules). "
        "Empfehlung: als separate Issues im Repo anlegen.", s))

    story.append(Spacer(1, 5 * mm))
    story += section_header("3.1 · Konkrete Code-Änderungen (Quick Wins)", s)

    story.append(subsection("T1b – TYPE_MAP ändern", s))
    story.append(code_block(
        '# src/transform.py — TYPE_MAP\n'
        '# Vorher:\n'
        '"tinyint":  "TINYINT",\n'
        '# Nachher:\n'
        '"tinyint":  "TINYINT UNSIGNED",', s))
    story.append(Spacer(1, 3 * mm))

    story.append(subsection("T5b – N-String-Literale in convert_view_sql()", s))
    story.append(code_block(
        "# src/transform.py — convert_view_sql(), nach den Typ-Ersetzungen\n"
        "# N'...' Unicode-Literale entfernen (MySQL: alle Strings sind UTF-8)\n"
        "sql = re.sub(r\"\\bN(?=')\", '', sql)", s))
    story.append(Spacer(1, 3 * mm))

    story.append(subsection("V5 – CTE-Versions-Hinweis in convert_view_sql()", s))
    story.append(code_block(
        "# src/transform.py — convert_view_sql(), nach Header-Entfernung\n"
        "if re.search(r'\\bWITH\\b', sql, re.IGNORECASE):\n"
        "    warnings.append(\n"
        "        'CTE verwendet (WITH ...): erfordert MySQL >= 8.0 / MariaDB >= 10.2'\n"
        "    )", s))


# ════════════════════════════════════════════════════════════════════════════
#  Abschnitt 4 – Akzeptanzkriterien
# ════════════════════════════════════════════════════════════════════════════
def build_section4(s, story):
    story += section_header("4 · Akzeptanzkriterien", s)
    story.append(body(
        "Erfolg wird auf zwei Ebenen gemessen: Unit-Tests im Repo und "
        "Deploy-Test gegen eine leere MariaDB-Instanz.", s))
    story.append(Spacer(1, 3 * mm))

    rows = [
        ["T1b", "TINYINT",
         "TYPE_MAP['tinyint'] == 'TINYINT UNSIGNED'",
         "test_transform.py"],
        ["T5b", "N-String",
         "convert_view_sql(\"WHERE x = N'abc'\") → keine N' im Output",
         "test_transform.py"],
        ["V5",  "CTE-Hint",
         "convert_view_sql('WITH cte AS (...) SELECT ...') → warnings enthält 'CTE'",
         "test_transform.py"],
        ["X2",  "Trigger",
         "generate_mysql_ddl() mit Trigger-DDL → Kommentar '-- ⚠ MANUELL' im Output",
         "test_transform.py\ntest_mssql.py"],
        ["P4",  "Engine-Flag",
         "Flagging-Report am Ende des DDL enthält LastChange-Trigger-Namen",
         "test_transform.py"],
        ["Alle", "Smoke-Test",
         "Geniertes DDL deploybar auf MariaDB 10.6 ohne Fehler\n"
         "(py -m pytest + manueller Deploy-Test)",
         "tests/ + MariaDB"],
    ]

    usable = PAGE_W - 2 * MARGIN
    story.append(grid_table(
        ["ID", "Punkt", "Test-Kriterium", "Test-Datei"],
        rows, s,
        col_widths=[12*mm, 14*mm, usable - 12 - 14 - 42*mm, 42*mm],
    ))
    story.append(Spacer(1, 4 * mm))

    story.append(ok_box(
        "<b>Aktueller Test-Stand:</b> 189 Tests, alle grün (Branch master). "
        "Neue Tests für T1b, T5b, V5 können als Ergänzung zu den bestehenden "
        "test_transform.py-Tests hinzugefügt werden.", s))


# ════════════════════════════════════════════════════════════════════════════
#  Build
# ════════════════════════════════════════════════════════════════════════════
def build_pdf():
    doc = SimpleDocTemplate(
        OUT_FILE,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=20 * mm, bottomMargin=18 * mm,
    )

    s     = make_styles()
    story = []

    build_cover(s, story)
    story.append(Spacer(1, 6 * mm))
    build_section1(s, story)
    story.append(Spacer(1, 4 * mm))
    build_section2(s, story)
    story.append(Spacer(1, 4 * mm))
    build_section3(s, story)
    story.append(Spacer(1, 4 * mm))
    build_section4(s, story)

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    print(f"PDF gespeichert: {OUT_FILE}")


if __name__ == "__main__":
    build_pdf()

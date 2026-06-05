"""
MDF-to-MySQL Migration Tool
Dokumentation: Steuerungsoptionen — Deploy & Datenmigration

Ausfuehren:  py generate_steuerung_pdf.py
Ausgabe:     MDF_Migration_Tool_Steuerung.pdf
"""
import os
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)

# ── Farben ────────────────────────────────────────────────────────────────────
NAVY       = colors.HexColor('#0D2545')
DARK_BLUE  = colors.HexColor('#1A3A6E')
MID_BLUE   = colors.HexColor('#2E5FA3')
LIGHT_BLUE = colors.HexColor('#EBF1FB')
GOLD       = colors.HexColor('#C8A951')
GOLD_LIGHT = colors.HexColor('#F5E9C4')
WHITE      = colors.white
GREY_DARK  = colors.HexColor('#2C3E50')
GREY_MID   = colors.HexColor('#7F8C8D')
GREY_LIGHT = colors.HexColor('#F4F6F8')
GREEN      = colors.HexColor('#1B5E20')
GREEN_MID  = colors.HexColor('#2E7D32')
GREEN_LIGHT= colors.HexColor('#E8F5E9')
RED        = colors.HexColor('#7F0000')
RED_MID    = colors.HexColor('#C62828')
RED_LIGHT  = colors.HexColor('#FFEBEE')
AMBER      = colors.HexColor('#E65100')

OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      'MDF_Migration_Tool_Steuerung.pdf')
W, H = A4
PAGE_NUM = [0]


# ── Header / Footer ───────────────────────────────────────────────────────────
def on_page(canvas, doc):
    PAGE_NUM[0] += 1
    canvas.saveState()
    # Goldene Leiste oben
    canvas.setFillColor(GOLD)
    canvas.rect(0, H - 8*mm, W, 3*mm, fill=1, stroke=0)
    if PAGE_NUM[0] > 1:
        canvas.setFillColor(DARK_BLUE)
        canvas.setFont('Helvetica-Bold', 8)
        canvas.drawString(20*mm, H - 6*mm,
                          'MDF → MySQL Migration Tool  —  Steuerungsoptionen')
        canvas.setFont('Helvetica', 8)
        canvas.setFillColor(GREY_MID)
        canvas.drawRightString(W - 20*mm, H - 6*mm,
                               'Version 2.0  |  Juni 2026')
        canvas.setStrokeColor(GOLD)
        canvas.setLineWidth(0.5)
        canvas.line(20*mm, 14*mm, W - 20*mm, 14*mm)
        canvas.setFillColor(GREY_MID)
        canvas.setFont('Helvetica', 8)
        canvas.drawString(20*mm, 9*mm,
                          'feat/#13-schema-diff-deploy')
        canvas.drawRightString(W - 20*mm, 9*mm,
                               f'Seite {PAGE_NUM[0] - 1}')
    canvas.restoreState()


# ── Styles ────────────────────────────────────────────────────────────────────
def PS(name, **kw):
    return ParagraphStyle(name, **kw)


def make_styles():
    return {
        'cover_title': PS('ct', fontName='Helvetica-Bold', fontSize=28,
            textColor=WHITE, alignment=TA_CENTER, leading=36),
        'cover_sub': PS('cs', fontName='Helvetica', fontSize=13,
            textColor=GOLD, alignment=TA_CENTER, leading=18),
        'cover_meta': PS('cm', fontName='Helvetica', fontSize=10,
            textColor=colors.HexColor('#A0B4CC'), alignment=TA_CENTER),
        'cover_toc_head': PS('cth', fontName='Helvetica-Bold', fontSize=10,
            textColor=GOLD, alignment=TA_LEFT),
        'cover_toc': PS('ct2', fontName='Helvetica', fontSize=9.5,
            textColor=colors.HexColor('#C8D8E8'), alignment=TA_LEFT, leading=16),
        'h2': PS('h2', fontName='Helvetica-Bold', fontSize=13,
            textColor=DARK_BLUE, spaceBefore=10, spaceAfter=5),
        'h3': PS('h3', fontName='Helvetica-Bold', fontSize=11,
            textColor=DARK_BLUE, spaceBefore=8, spaceAfter=4),
        'body': PS('body', fontName='Helvetica', fontSize=10,
            textColor=GREY_DARK, leading=15, spaceAfter=5,
            alignment=TA_JUSTIFY),
        'bullet': PS('bul', fontName='Helvetica', fontSize=10,
            textColor=GREY_DARK, leading=14, spaceAfter=3,
            leftIndent=14, firstLineIndent=-10),
        'sub_bullet': PS('sbul', fontName='Helvetica', fontSize=9.5,
            textColor=GREY_DARK, leading=13, spaceAfter=2,
            leftIndent=26, firstLineIndent=-10),
        'label_green': PS('lg', fontName='Helvetica-Bold', fontSize=10,
            textColor=WHITE, leading=14),
        'label_red': PS('lr', fontName='Helvetica-Bold', fontSize=10,
            textColor=WHITE, leading=14),
        'note': PS('note', fontName='Helvetica-Oblique', fontSize=9,
            textColor=GREY_MID, leading=13, spaceAfter=4, leftIndent=14),
        'warn_text': PS('wt', fontName='Helvetica-Bold', fontSize=9.5,
            textColor=RED_MID, leading=14),
        'info_text': PS('it', fontName='Helvetica', fontSize=9.5,
            textColor=DARK_BLUE, leading=14),
        'small': PS('sm', fontName='Helvetica', fontSize=8,
            textColor=GREY_MID, alignment=TA_CENTER),
        'sub_label': PS('sl', fontName='Helvetica-Bold', fontSize=9.5,
            textColor=GREY_DARK, spaceBefore=5, spaceAfter=2,
            leftIndent=14),
    }


# ── Bausteine ─────────────────────────────────────────────────────────────────

def section_header(number, title):
    """Dunkelblauer Abschnitts-Header mit goldener Unterlinie."""
    text = f'{number}.  {title}'
    t = Table([[Paragraph(text, PS('sh', fontName='Helvetica-Bold',
                fontSize=13, textColor=WHITE))]],
              colWidths=[W - 40*mm])
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), DARK_BLUE),
        ('TOPPADDING',    (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING',   (0,0), (-1,-1), 12),
        ('LINEBELOW',     (0,0), (-1,-1), 2, GOLD),
    ]))
    return [Spacer(1, 5*mm), t, Spacer(1, 4*mm)]


def option_bar(letter, title, color):
    """Farbiger Option-A / Option-B Label-Balken."""
    label_style = PS('ol', fontName='Helvetica-Bold', fontSize=10,
                     textColor=WHITE, leading=14)
    t = Table([[Paragraph(f'Option {letter}  —  {title}', label_style)]],
              colWidths=[W - 40*mm])
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), color),
        ('TOPPADDING',    (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('LEFTPADDING',   (0,0), (-1,-1), 12),
        ('ROUNDEDCORNERS', [4]),
    ]))
    return [t, Spacer(1, 3*mm)]


def info_box(text, S, bg=LIGHT_BLUE, text_color=DARK_BLUE, border=MID_BLUE):
    """Farbige Info-Box mit linkem Akzent-Streifen."""
    style = PS('ib', fontName='Helvetica', fontSize=9.5,
               textColor=text_color, leading=14)
    inner = Table([[Paragraph(text, style)]],
                  colWidths=[W - 40*mm - 8*mm])
    inner.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), bg),
        ('TOPPADDING',    (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING',   (0,0), (-1,-1), 10),
        ('RIGHTPADDING',  (0,0), (-1,-1), 10),
    ]))
    # Streifen links + Box
    outer = Table([[''  , inner]],
                  colWidths=[5*mm, W - 40*mm - 5*mm])
    outer.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (0,-1), border),
        ('BACKGROUND',    (1,0), (1,-1), bg),
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    return [outer, Spacer(1, 3*mm)]


def warn_box(text, S):
    return info_box(text, S, bg=RED_LIGHT, text_color=RED_MID, border=RED_MID)


def green_box(text, S):
    return info_box(text, S, bg=GREEN_LIGHT, text_color=GREEN, border=GREEN_MID)


def data_table(rows, col_widths):
    """Standard-Tabelle mit blauem Header und alternierenden Zeilen."""
    header_style = PS('th', fontName='Helvetica-Bold', fontSize=9.5,
                      textColor=WHITE, leading=13)
    cell_style   = PS('td', fontName='Helvetica', fontSize=9.5,
                      textColor=GREY_DARK, leading=13)

    table_data = []
    for r_idx, row in enumerate(rows):
        table_row = []
        for cell in row:
            style = header_style if r_idx == 0 else cell_style
            table_row.append(Paragraph(str(cell), style))
        table_data.append(table_row)

    t = Table(table_data, colWidths=col_widths)
    t.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  DARK_BLUE),
        ('LINEBELOW',     (0,0), (-1,0),  1.5, GOLD),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [WHITE, LIGHT_BLUE]),
        ('GRID',          (0,0), (-1,-1), 0.4,
         colors.HexColor('#B0BEC5')),
        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
        ('TOPPADDING',    (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('LEFTPADDING',   (0,0), (-1,-1), 8),
        ('RIGHTPADDING',  (0,0), (-1,-1), 8),
    ]))
    return t


def li(text, S):
    return Paragraph(f'&#x2022;&#160;&#160;{text}', S['bullet'])


def sub_li(text, S):
    return Paragraph(f'&#x2013;&#160;&#160;{text}', S['sub_bullet'])


def sp(n=4):
    return Spacer(1, n*mm)


# ── Cover ─────────────────────────────────────────────────────────────────────
def build_cover(S):
    e = []
    e.append(sp(7))

    # Goldene Trennlinie oben
    bar = Table([['']], colWidths=[W - 40*mm], rowHeights=[4])
    bar.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,-1), GOLD)]))
    e.append(bar)
    e.append(sp(8))

    # Haupttitel
    e.append(Paragraph('MDF &#x2192; MySQL<br/>Migration Tool', S['cover_title']))
    e.append(sp(4))
    e.append(Paragraph('Steuerungsoptionen: Deploy &amp; Datenmigration',
                        S['cover_sub']))
    e.append(sp(3))
    e.append(Paragraph('Version 2.0  |  Branch: feat/#13-schema-diff-deploy  |  Juni 2026',
                        S['cover_meta']))
    e.append(sp(6))

    # Goldene Trennlinie
    bar2 = Table([['']], colWidths=[W - 40*mm], rowHeights=[2])
    bar2.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,-1), GOLD)]))
    e.append(bar2)
    e.append(sp(8))

    # Inhaltsverzeichnis-Box
    toc_data = [
        ['Inhalt dieser Dokumentation'],
        ['1.  Übersicht der Steuerelemente'],
        ['2.  Deploy-Modus  (Schema-Diff vs. Vollständig neu)'],
        ['3.  Datenmigrations-Modus  (Scope-Steuerung)'],
        ['4.  Typische Anwendungsfälle'],
        ['5.  Dry-Run Modus'],
        ['6.  Sicherheitsregeln'],
        ['7.  Checkpoint &amp; Resume'],
    ]
    toc_styles = []
    for i, row in enumerate(toc_data):
        st = S['cover_toc_head'] if i == 0 else S['cover_toc']
        toc_styles.append([Paragraph(row[0], st)])

    toc = Table(toc_styles, colWidths=[W - 40*mm])
    toc.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('BACKGROUND',    (0,0), (-1,0),  colors.HexColor('#0D1E3A')),
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING',   (0,0), (-1,-1), 14),
        ('LINEBELOW',     (0,0), (-1,0),  1, GOLD),
        ('LINEABOVE',     (0,0), (-1,0),  1, GOLD),
    ]))
    e.append(toc)
    e.append(sp(8))

    e.append(Paragraph('github.com/esternu/mdf-to-mysql-migration',
                        S['small']))
    e.append(PageBreak())
    return e


# ── Abschnitt 1: Übersicht ────────────────────────────────────────────────────
def build_section1(S):
    e = []
    e += section_header('1', 'Übersicht')
    e.append(Paragraph(
        'Das Tool unterstützt zwei unabhängige Aktionen, die einzeln oder '
        'kombiniert ausgeführt werden können.',
        S['body']
    ))
    e.append(sp(3))

    rows = [
        ['Aktion', 'Steuerelement', 'Wirkung'],
        ['Schema deployen',
         'Checkbox\n"Schema-Diff (inkrementell)"',
         'Datenbankstruktur aktualisieren\n(Tabellen, Spalten, Indexes, FKs)'],
        ['Daten übertragen',
         'Checkbox "Daten übertragen"\n+ OptionMenu Scope',
         'Tabelleninhalte aus MDF\nnach MySQL übertragen'],
    ]
    e.append(data_table(rows, [4*mm*9, 5*mm*9, (W-40*mm) - 4*mm*9 - 5*mm*9]))
    e.append(sp(4))
    return e


# ── Abschnitt 2: Deploy-Modus ─────────────────────────────────────────────────
def build_section2(S):
    e = []
    e += section_header('2', 'Deploy-Modus (Schema)')
    e.append(Paragraph(
        'Gesteuert durch die Checkbox <b>"Schema-Diff (inkrementell)"</b> '
        'in der Aktionsleiste.',
        S['body']
    ))
    e.append(sp(3))

    # Option A
    e += option_bar('A', 'Schema-Diff  (Standard: Checkbox aktiviert)', GREEN_MID)
    e.append(li('Vergleicht das MDF-Schema mit dem bestehenden MySQL-Schema', S))
    e.append(li('Ermittelt nur die Unterschiede: neue Tabellen, neue Spalten, '
                'Typ-Änderungen', S))
    e.append(li('Generiert <font name="Courier" size="9">ALTER TABLE</font> / '
                '<font name="Courier" size="9">CREATE TABLE IF NOT EXISTS</font>', S))
    e.append(li('Bestehende Daten bleiben vollständig erhalten', S))
    e.append(li('Dry-Run zeigt den vollständigen Diff-Preview — ohne '
                'Änderungen auszuführen', S))
    e.append(li('Im Log erscheint eine Zusammenfassung der geplanten Änderungen', S))
    e.append(sp(4))

    # Option B
    e += option_bar('B', 'Vollständig neu  (Checkbox deaktiviert)', RED_MID)
    e.append(li('Generiert <font name="Courier" size="9">DROP TABLE IF EXISTS</font> + '
                '<font name="Courier" size="9">CREATE TABLE</font> für alle Tabellen', S))
    e += warn_box('ALLE bestehenden Daten in der Zieldatenbank werden gelöscht!', S)
    e.append(li('Erfordert explizite Bestätigung mit deutlichem Warnhinweis', S))
    e.append(li('Geeignet für: Erstmigration, vollständiger Reset der Datenbank', S))
    e.append(sp(4))
    return e


# ── Abschnitt 3: Datenmigrations-Modus ───────────────────────────────────────
def build_section3(S):
    e = []
    e += section_header('3', 'Datenmigrations-Modus')
    e.append(Paragraph(
        'Aktiviert durch Checkbox <b>"Daten übertragen"</b>. '
        'Das OptionMenu daneben steuert den Scope.',
        S['body']
    ))
    e.append(sp(3))

    # Option A
    e += option_bar('A', 'Nur geänderte Tabellen  (Standard)', GREEN_MID)
    e.append(Paragraph('<b>Was neu geladen wird:</b>', S['sub_label']))
    e.append(sub_li('Neue Tabellen &#x2192; Erstbefüllung (kein TRUNCATE nötig)', S))
    e.append(sub_li('Tabellen mit neuen Spalten &#x2192; TRUNCATE + INSERT '
                    '(neue Spalte aus MDF befüllt)', S))
    e.append(sub_li('Tabellen mit Typ-Änderungen &#x2192; TRUNCATE + INSERT '
                    '(Inkompatibilität vermeiden)', S))
    e.append(sp(2))
    e.append(Paragraph('<b>Was unangetastet bleibt:</b>', S['sub_label']))
    e.append(sub_li('Tabellen mit nur neuen Indexes / FKs &#x2192; Daten bleiben erhalten', S))
    e.append(sub_li('Unveränderte Tabellen &#x2192; werden nicht angefasst', S))
    e.append(sp(4))

    # Option B
    e += option_bar('B', 'Alle Tabellen', RED_MID)
    e.append(li('<font name="Courier" size="9">TRUNCATE + INSERT</font> '
                'für jede Tabelle', S))
    e += warn_box('Alle bestehenden Daten werden durch MDF-Daten ersetzt.', S)
    e.append(li('Geeignet für: vollständige Datensynchronisation, Erstmigration', S))
    e.append(sp(4))
    return e


# ── Abschnitt 4: Anwendungsfälle ─────────────────────────────────────────────
def build_section4(S):
    e = []
    e += section_header('4', 'Typische Anwendungsfälle')

    rows = [
        ['Szenario', 'Schema-Diff', 'Daten\nübertragen', 'Scope'],
        ['Erstmigration\n(leere MySQL-DB)',
         'Vollständig neu\n(Checkbox aus)', 'Ja', 'Alle\nTabellen'],
        ['Schema erweitern\n(neue Spalte)',
         'Inkrementell\n(Checkbox an)', 'Ja', 'Nur\ngeänderte'],
        ['Schema prüfen\n(Was würde sich ändern?)',
         'Inkrementell\n+ Dry-Run', 'Nein', '—'],
        ['Nur Daten synchronisieren\n(Schema bereits aktuell)',
         'Inkrementell\n(kein Deploy)', 'Ja', 'Nur\ngeänderte'],
        ['Vollständiger Reset\n(Daten + Struktur)',
         'Vollständig neu\n(Checkbox aus)', 'Ja', 'Alle\nTabellen'],
    ]
    col_w = [5.5*cm, 4.5*cm, 2.5*cm, (W-40*mm) - 12.5*cm]
    tbl = data_table(rows, col_w)
    # Zeilen grün/rot je nach Szenario
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,1), (-1,1), colors.HexColor('#FFF3F3')),
        ('BACKGROUND', (0,2), (-1,2), colors.HexColor('#F3FFF3')),
        ('BACKGROUND', (0,3), (-1,3), colors.HexColor('#F3FFF3')),
        ('BACKGROUND', (0,4), (-1,4), colors.HexColor('#F3FFF3')),
        ('BACKGROUND', (0,5), (-1,5), colors.HexColor('#FFF3F3')),
    ]))
    e.append(tbl)
    e.append(sp(4))
    return e


# ── Abschnitt 5: Dry-Run ─────────────────────────────────────────────────────
def build_section5(S):
    e = []
    e += section_header('5', 'Dry-Run Modus')
    e += info_box(
        'Die Checkbox "Dry-Run" kann jederzeit zusätzlich aktiviert werden. '
        'Es werden KEINE Änderungen an MySQL ausgeführt.',
        S
    )
    e.append(li('Diff wird berechnet und vollständig im Log angezeigt', S))
    e.append(li('Zeigt welche Tabellen / Spalten sich ändern würden', S))
    e.append(li('"Daten übertragen" aktiv: zeigt welche Tabellen '
                'neu geladen würden', S))
    e.append(li('Ideal zur Prüfung vor dem echten Deploy', S))
    e.append(sp(4))
    return e


# ── Abschnitt 6: Sicherheitsregeln ───────────────────────────────────────────
def build_section6(S):
    e = []
    e += section_header('6', 'Sicherheitsregeln')
    e += warn_box(
        'Folgende Elemente werden im Schema-Diff-Modus niemals automatisch '
        'entfernt — auch wenn sie in der MDF nicht mehr vorhanden sind.',
        S
    )
    e.append(li('Tabellen, die in MySQL existieren, aber nicht mehr in der MDF', S))
    e.append(li('Spalten, die in MySQL existieren, aber nicht mehr in der MDF', S))
    e.append(li('Views (werden separat über das vollständige DDL behandelt)', S))
    e.append(sp(2))
    e.append(Paragraph(
        'Diese Elemente erscheinen im Log als Warnung. '
        'Ein manuelles DROP TABLE / DROP COLUMN ist erforderlich.',
        S['note']
    ))
    e.append(sp(4))
    return e


# ── Abschnitt 7: Checkpoint & Resume ─────────────────────────────────────────
def build_section7(S):
    e = []
    e += section_header('7', 'Checkpoint &amp; Resume')
    e.append(li('Button <b>"&#x23E9; Resume"</b> erscheint automatisch, '
                'wenn eine Checkpoint-Datei existiert', S))
    e.append(li('Beim Resume werden bereits migrierte Tabellen übersprungen', S))
    e.append(li('Checkpoint wird nach erfolgreichem Abschluss automatisch gelöscht', S))
    e.append(li('Checkpoint-Datei: '
                '<font name="Courier" size="9">temp/migration_checkpoint.json</font>', S))
    e.append(sp(3))
    e += green_box(
        'Der Resume-Button ist unabhängig vom gewählten Deploy- oder '
        'Datenmigrations-Modus verfügbar.',
        S
    )
    e.append(sp(6))

    # Abschluss-Linie
    bar = Table([['']], colWidths=[W - 40*mm], rowHeights=[2])
    bar.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,-1), GOLD)]))
    e.append(bar)
    e.append(sp(2))
    e.append(Paragraph('github.com/esternu/mdf-to-mysql-migration', S['small']))
    return e


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    doc = SimpleDocTemplate(
        OUTPUT,
        pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=18*mm,  bottomMargin=20*mm,
        title='MDF-to-MySQL Migration Tool – Steuerungsoptionen',
        author='MDF Migration Tool',
    )

    S = make_styles()
    story = []
    story += build_cover(S)
    story += build_section1(S)
    story += build_section2(S)
    story += build_section3(S)
    story += build_section4(S)
    story += build_section5(S)
    story += build_section6(S)
    story += build_section7(S)

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    print(f'PDF erstellt: {OUTPUT}')


if __name__ == '__main__':
    main()

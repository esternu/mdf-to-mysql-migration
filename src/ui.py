"""
tkinter GUI für das MDF-to-MySQL Migration Tool.
Enthält die App-Klasse mit allen vier Tabs und der Konfigurations-Verwaltung.
"""
import base64
import datetime
import json
import os
import re
import subprocess as _subprocess
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional

# Interne Module
from paths        import CFG_FILE, LOG_FILE, TEMP_DIR, CHECKPOINT_FILE
from mssql        import attach_mdf, detach_and_cleanup, get_mssql_drivers, PYODBC_OK
from transform    import generate_mysql_ddl
from deploy       import deploy_to_mysql, MYSQL_OK
from migrate_data import (get_table_list, migrate_all, CHUNK_SIZE,
                          checkpoint_exists, delete_checkpoint, load_checkpoint)
from schema_diff  import (read_mysql_schema, diff_schemas,
                          generate_diff_ddl, format_diff_summary,
                          get_tables_to_refresh)

try:
    import mysql.connector
except ImportError:
    mysql = None   # type: ignore


# ════════════════════════════════════════════════════════════════════════════
#  App
# ════════════════════════════════════════════════════════════════════════════
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MDF → MySQL Migration Tool")
        self.geometry("1230x760")
        self.resizable(True, True)
        self._stop_event = threading.Event()
        self._build_ui()
        self._load_config()
        self._check_deps()
        self._refresh_resume_btn()

    # ── LED-Hilfsmethoden ────────────────────────────────────────────────
    # Zustände: "idle"=grau, "running"=gelb, "ok"=grün, "error"=rot
    _LED_COLORS = {"idle": "#aaaaaa", "running": "#e6c200", "ok": "#22aa44", "error": "#cc2222"}
    _LED_SIZE   = 14   # px

    def _make_led(self, parent) -> tk.Canvas:
        # ttk.Frame hat kein -background → Hintergrund vom Root-Fenster holen
        bg = self.cget("background")
        c = tk.Canvas(parent, width=self._LED_SIZE, height=self._LED_SIZE,
                      highlightthickness=0, bd=0, bg=bg)
        c.create_oval(2, 2, self._LED_SIZE - 2, self._LED_SIZE - 2,
                      fill=self._LED_COLORS["idle"], outline="#777", tags="led")
        return c

    def _set_led(self, canvas: tk.Canvas, state: str):
        color = self._LED_COLORS.get(state, self._LED_COLORS["idle"])
        canvas.itemconfig("led", fill=color)

    # ── UI-Aufbau ────────────────────────────────────────────────────────
    def _build_ui(self):
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=8, pady=8)

        self.tab_src = ttk.Frame(nb)
        self.tab_dst = ttk.Frame(nb)
        self.tab_ddl = ttk.Frame(nb)
        self.tab_log = ttk.Frame(nb)

        nb.add(self.tab_src, text=" 1 · Quelle (.mdf) ")
        nb.add(self.tab_dst, text=" 2 · Ziel (MySQL)  ")
        nb.add(self.tab_ddl, text=" 3 · DDL-Vorschau  ")
        nb.add(self.tab_log, text=" 4 · Log           ")

        self._build_source_tab()
        self._build_dest_tab()
        self._build_ddl_tab()
        self._build_log_tab()

        # ── Aktions-Buttons mit Status-LEDs ──────────────────────────────
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=8, pady=(0, 2))

        # Schema lesen
        self._led_schema = self._make_led(btn_frame)
        self._led_schema.pack(side="left", padx=(4, 2), pady=4)
        ttk.Button(btn_frame, text="Schema lesen", command=self._read_schema).pack(side="left", padx=(0, 8))

        # DDL generieren
        self._led_ddl = self._make_led(btn_frame)
        self._led_ddl.pack(side="left", padx=(4, 2), pady=4)
        ttk.Button(btn_frame, text="DDL generieren", command=self._generate_ddl).pack(side="left", padx=(0, 2))
        ttk.Button(btn_frame, text="DDL speichern …", command=self._save_ddl).pack(side="left", padx=(0, 8))

        # Deploy
        self._led_deploy = self._make_led(btn_frame)
        self._led_deploy.pack(side="left", padx=(4, 2), pady=4)
        ttk.Button(btn_frame, text="▶ Auf MySQL deployen", command=self._deploy).pack(side="left", padx=(0, 4))

        # Deploy-Modus: inkrementell (ALTER TABLE) oder vollständig (DROP+CREATE)
        self._incremental_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(btn_frame, text="Schema-Diff (inkrementell)",
                        variable=self._incremental_var).pack(side="left", padx=(0, 2))

        # Daten-Modus: Checkbox + OptionMenu (Alle / Nur geänderte)
        self._transfer_data_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(btn_frame, text="Daten übertragen",
                        variable=self._transfer_data_var,
                        command=self._refresh_data_scope_state).pack(side="left", padx=(8, 2))
        # "Alle" = alle Tabellen neu befüllen (TRUNCATE+INSERT)
        # "Nur geänderte" = nur Tabellen aus Schema-Diff (neue + geänderte Spalten)
        self._data_scope_var = tk.StringVar(value="diff")
        self._data_scope_menu = ttk.OptionMenu(
            btn_frame, self._data_scope_var,
            "diff",
            "diff",   "Nur geänderte",
            "all",    "Alle Tabellen",
        )
        self._data_scope_menu.configure(width=13)
        self._data_scope_menu.pack(side="left", padx=(0, 2))

        self._dry_run_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(btn_frame, text="Dry-Run",
                        variable=self._dry_run_var).pack(side="left", padx=(4, 2))

        # Cancel / Resume / Deps
        self._cancel_btn = ttk.Button(btn_frame, text="⏹ Abbrechen",
                                      command=self._cancel_migration, state="disabled")
        self._cancel_btn.pack(side="left", padx=4)
        self._resume_btn = ttk.Button(btn_frame, text="⏩ Resume",
                                      command=self._resume_migration, state="disabled")
        self._resume_btn.pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Abhängigkeiten prüfen",
                   command=self._check_deps).pack(side="right", padx=4)

        # ── Fortschrittsbalken ────────────────────────────────────────────
        progress_frame = ttk.Frame(self)
        progress_frame.pack(fill="x", padx=8, pady=(0, 2))
        self._progress_var   = tk.DoubleVar(value=0.0)
        self._progress_bar   = ttk.Progressbar(progress_frame, variable=self._progress_var,
                                               maximum=100, mode="determinate")
        self._progress_bar.pack(fill="x", side="left", expand=True, padx=(0, 8))
        self._progress_label = ttk.Label(progress_frame, text="", width=42, anchor="w")
        self._progress_label.pack(side="left")

        # Konfig-Leiste
        cfg_frame = ttk.Frame(self)
        cfg_frame.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Label(cfg_frame, text="Konfiguration:").pack(side="left", padx=(4, 8))
        ttk.Label(cfg_frame, text="Profil:").pack(side="left")
        self._profile_var   = tk.StringVar(value="Standard")
        self._profile_combo = ttk.Combobox(cfg_frame, textvariable=self._profile_var, width=18)
        self._profile_combo.pack(side="left", padx=4)
        ttk.Button(cfg_frame, text="💾  Speichern", command=self._save_config).pack(side="left", padx=4)
        ttk.Button(cfg_frame, text="📂  Laden",     command=self._load_config).pack(side="left", padx=4)
        ttk.Button(cfg_frame, text="🗑  Löschen",   command=self._delete_profile).pack(side="left", padx=4)
        self._cfg_status = ttk.Label(cfg_frame, text="", foreground="#555")
        self._cfg_status.pack(side="left", padx=8)
        self._refresh_profiles()

    def _build_source_tab(self):
        f = self.tab_src
        f.columnconfigure(1, weight=1)   # Spalte 1 (Eingabefelder) dehnt sich mit

        # Zeile 0: Label
        ttk.Label(f, text=".mdf Datei:").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 0))

        # Zeile 1: Pfadfeld – fast volle Breite, sticky EW
        self.mdf_path = tk.StringVar()
        ttk.Entry(f, textvariable=self.mdf_path).grid(
            row=1, column=0, columnspan=3, sticky="ew", padx=8, pady=(2, 0))

        # Zeile 2: Durchsuchen-Button linksbündig
        ttk.Button(f, text="Durchsuchen …", command=self._browse_mdf).grid(
            row=2, column=0, sticky="w", padx=8, pady=(2, 8))

        # Zeile 3: DB-Name
        ttk.Label(f, text="Datenbank-Name (intern):").grid(row=3, column=0, sticky="w", padx=8, pady=6)
        self.db_attach_name = tk.StringVar(value="MigratedDB")
        ttk.Entry(f, textvariable=self.db_attach_name, width=36).grid(
            row=3, column=1, sticky="w", padx=4)

        # Zeile 4: ODBC-Treiber
        ttk.Label(f, text="ODBC-Treiber:").grid(row=4, column=0, sticky="w", padx=8, pady=6)
        self.driver_var   = tk.StringVar()
        self.driver_combo = ttk.Combobox(f, textvariable=self.driver_var)
        self.driver_combo.grid(row=4, column=1, sticky="ew", padx=4, pady=6)
        ttk.Button(f, text="Treiber aktualisieren", command=self._refresh_drivers).grid(
            row=4, column=2, padx=8)

        # Zeile 5: Hinweis
        info = (
            "Hinweis: Zum Lesen der .mdf-Datei wird Microsoft SQL Server LocalDB\n"
            "oder SQL Server Express benötigt (kostenlos bei Microsoft erhältlich).\n"
            "Installer: https://aka.ms/sqllocaldb\n\n"
            "Alternativ: DDL-Datei manuell aus SQL Server Management Studio exportieren\n"
            "und im Tab '3 · DDL-Vorschau' einfügen."
        )
        ttk.Label(f, text=info, foreground="#555", justify="left").grid(
            row=5, column=0, columnspan=3, padx=8, pady=12, sticky="w")
        self._refresh_drivers()

    def _build_dest_tab(self):
        f      = self.tab_dst
        fields = [
            ("MySQL Host (Synology IP):", "mysql_host", "192.168.1.x"),
            ("Port:",                     "mysql_port", "3306"),
            ("Benutzer:",                 "mysql_user", "root"),
            ("Passwort:",                 "mysql_pass", ""),
            ("Ziel-Datenbankname:",       "mysql_db",   "migrated_db"),
        ]
        for i, (label, attr, placeholder) in enumerate(fields):
            ttk.Label(f, text=label).grid(row=i, column=0, sticky="w", padx=8, pady=6)
            var = tk.StringVar(value=placeholder if attr != "mysql_pass" else "")
            setattr(self, attr, var)
            show = "*" if attr == "mysql_pass" else ""
            ttk.Entry(f, textvariable=var, width=40, show=show).grid(
                row=i, column=1, padx=4, pady=6, sticky="w")
        ttk.Button(f, text="Verbindung testen", command=self._test_mysql).grid(
            row=len(fields), column=1, sticky="w", padx=4, pady=10)
        ttk.Label(f,
            text="Synology: MariaDB/MySQL-Paket im Paket-Zentrum aktivieren,\n"
                 "Remote-Zugriff in phpMyAdmin oder SSH erlauben.",
            foreground="#555", justify="left",
        ).grid(row=len(fields)+1, column=0, columnspan=2, padx=8, pady=8, sticky="w")

    def _build_ddl_tab(self):
        f = self.tab_ddl
        self.ddl_text = scrolledtext.ScrolledText(f, font=("Consolas", 9), wrap="none")
        self.ddl_text.pack(fill="both", expand=True, padx=4, pady=4)
        ttk.Label(f, text="DDL hier direkt bearbeiten oder manuell einfügen.",
                  foreground="#555").pack(anchor="w", padx=4)

    def _build_log_tab(self):
        f = self.tab_log

        path_frame = ttk.Frame(f)
        path_frame.pack(fill="x", padx=4, pady=(4, 0))
        ttk.Label(path_frame, text="Log-Datei:").pack(side="left")
        self._log_path_var = tk.StringVar(value=LOG_FILE)
        ttk.Entry(path_frame, textvariable=self._log_path_var,
                  state="readonly", width=70).pack(side="left", padx=4)
        ttk.Button(path_frame, text="Im Explorer öffnen",
                   command=self._open_log_folder).pack(side="left", padx=2)

        self.log_text = scrolledtext.ScrolledText(
            f, font=("Consolas", 9), state="disabled", wrap="none")
        self.log_text.pack(fill="both", expand=True, padx=4, pady=4)

        self.log_text.tag_config("error",   foreground="#cc0000", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("warning", foreground="#b36200")
        self.log_text.tag_config("success", foreground="#006600", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("section", foreground="#00008b", font=("Consolas", 9, "bold"))
        self.log_text.tag_config("ts",      foreground="#888888")

        btn_frame = ttk.Frame(f)
        btn_frame.pack(fill="x", padx=4, pady=2)
        ttk.Button(btn_frame, text="Log leeren",   command=self._clear_log).pack(side="right", padx=2)
        ttk.Button(btn_frame, text="Log kopieren", command=self._copy_log).pack(side="right", padx=2)

        # Log-Datei initialisieren
        with open(LOG_FILE, "w", encoding="utf-8") as fh:
            fh.write(f"=== MDF-to-MySQL Migration Log  {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===\n")

    # ── Log-Hilfsmethoden ────────────────────────────────────────────────
    def log(self, msg: str):
        ts    = datetime.datetime.now().strftime("%H:%M:%S")
        lower = msg.lower().strip()

        if lower.startswith("fehler") or lower.startswith("error") or "fehler:" in lower:
            tag = "error"
        elif lower.startswith("⚠") or "warnung" in lower or lower.startswith("warning"):
            tag = "warning"
        elif lower.startswith("✓") or "erfolgreich" in lower or lower.startswith("fertig"):
            tag = "success"
        elif lower.startswith("──") or lower.startswith("=="):
            tag = "section"
        else:
            tag = None

        self.log_text.config(state="normal")
        self.log_text.insert("end", f"[{ts}] ", "ts")
        if tag:
            self.log_text.insert("end", msg + "\n", tag)
        else:
            self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.update_idletasks()

        try:
            with open(LOG_FILE, "a", encoding="utf-8") as fh:
                fh.write(f"[{ts}] {msg}\n")
        except OSError:
            pass

    def _clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.config(state="disabled")
        try:
            with open(LOG_FILE, "w", encoding="utf-8") as fh:
                fh.write(f"=== Log geleert  {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===\n")
        except OSError:
            pass

    # ── Fortschritt / Cancel ─────────────────────────────────────────────
    # ── Fortschritt-Hilfsmethoden ────────────────────────────────────────
    def _progress_start_indeterminate(self, label: str = ""):
        """Startet animierten Fortschrittsbalken (für Operationen ohne messbaren Fortschritt)."""
        self._progress_bar.config(mode="indeterminate")
        self._progress_bar.start(12)
        self._progress_label.config(text=label)

    def _progress_start_determinate(self, label: str = ""):
        """Wechselt zu bestimmbarem Balken und setzt auf 0%."""
        self._progress_bar.stop()
        self._progress_bar.config(mode="determinate")
        self._progress_var.set(0.0)
        self._progress_label.config(text=label)

    def _progress_finish(self, label: str = "", success: bool = True, auto_reset_ms: int = 3000):
        """Setzt Balken auf 100% (oder 0% bei Fehler) und plant automatischen Reset."""
        self._progress_bar.stop()
        self._progress_bar.config(mode="determinate")
        self._progress_var.set(100.0 if success else 0.0)
        self._progress_label.config(text=label)
        if auto_reset_ms > 0:
            self.after(auto_reset_ms, self._reset_progress)

    def _reset_progress(self):
        self._progress_bar.stop()
        self._progress_bar.config(mode="determinate")
        self._progress_var.set(0.0)
        self._progress_label.config(text="")

    def _set_progress_label(self, text: str):
        self._progress_label.config(text=text)

    def _set_progress_pct(self, done: int, total: int, label: str = ""):
        pct = (done / total * 100) if total > 0 else 0
        self._progress_var.set(pct)
        if label:
            self._progress_label.config(text=label)

    def _set_progress(self, table: str, rows_done: int, rows_total: int):
        """Chunk-Callback für Datenmigration."""
        pct = (rows_done / rows_total * 100) if rows_total > 0 else 0
        self._progress_var.set(pct)
        self._progress_label.config(text=f"{table}: {rows_done:,} / {rows_total:,} Zeilen")

    def _progress_callback(self, table: str, rows_done: int, rows_total: int):
        self.after(0, self._set_progress, table, rows_done, rows_total)

    def _deploy_progress_callback(self, done: int, total: int):
        self.after(0, self._set_progress_pct, done, total,
                   f"DDL: {done}/{total} Anweisungen")

    def _set_migration_running(self, running: bool):
        state = "normal" if running else "disabled"
        self._cancel_btn.config(state=state)

    def _cancel_migration(self):
        self._stop_event.set()
        self.log("⚠ Abbruch angefordert …")

    def _refresh_data_scope_state(self):
        """OptionMenu für Daten-Scope aktivieren/deaktivieren je nach Checkbox."""
        state = "normal" if self._transfer_data_var.get() else "disabled"
        self._data_scope_menu.configure(state=state)

    def _refresh_resume_btn(self):
        """Resume-Button aktivieren wenn Checkpoint-Datei existiert."""
        if checkpoint_exists(CHECKPOINT_FILE):
            cp    = load_checkpoint(CHECKPOINT_FILE)
            done  = len(cp.get("completed", []))
            since = cp.get("started_at", "?")
            self._resume_btn.config(state="normal")
            self._resume_btn.config(text=f"⏩ Resume ({done} ✓)")
            self.log(f"  Checkpoint: {done} Tabellen seit {since} migriert – Resume möglich.")
        else:
            self._resume_btn.config(state="disabled", text="⏩ Resume")

    def _resume_migration(self):
        """Startet Deploy mit vorhandenem Checkpoint (überspringt abgeschlossene Tabellen)."""
        if not messagebox.askyesno(
            "Resume Migration",
            "Migration mit Checkpoint fortsetzen?\n"
            "Bereits migrierte Tabellen werden übersprungen.",
        ):
            return
        self._deploy(resume=True)

    def _copy_log(self):
        content = self.log_text.get("1.0", "end").strip()
        self.clipboard_clear()
        self.clipboard_append(content)
        self.log("✓ Log in Zwischenablage kopiert.")

    def _open_log_folder(self):
        _subprocess.Popen(["explorer", "/select,", os.path.normpath(LOG_FILE)])

    # ── Quell-Tab Hilfsmethoden ──────────────────────────────────────────
    def _browse_mdf(self):
        path = filedialog.askopenfilename(
            title="MDF-Datei auswählen",
            filetypes=[("SQL Server Database", "*.mdf"), ("Alle Dateien", "*.*")],
        )
        if path:
            self.mdf_path.set(path)
            basename = os.path.splitext(os.path.basename(path))[0]
            self.db_attach_name.set(re.sub(r'[^a-zA-Z0-9_]', '_', basename))

    def _refresh_drivers(self):
        drivers = get_mssql_drivers()
        self.driver_combo["values"] = drivers
        if drivers:
            self.driver_var.set(drivers[0])

    def _check_deps(self):
        msgs = []
        if PYODBC_OK:
            msgs.append("✓ pyodbc installiert")
            drivers = get_mssql_drivers()
            if drivers:
                msgs.append(f"✓ ODBC-Treiber gefunden: {drivers[0]}")
            else:
                msgs.append("⚠ Kein SQL-Server-ODBC-Treiber gefunden")
                msgs.append("  → SQL Server LocalDB installieren: https://aka.ms/sqllocaldb")
        else:
            msgs.append("✗ pyodbc fehlt  → pip install pyodbc")

        if MYSQL_OK:
            msgs.append("✓ mysql-connector-python installiert")
        else:
            msgs.append("✗ mysql-connector-python fehlt  → pip install mysql-connector-python")

        self.log("── Abhängigkeiten ──")
        for m in msgs:
            self.log("  " + m)
        self.log("")

    # ── Aktionen ────────────────────────────────────────────────────────
    def _read_schema(self):
        if not PYODBC_OK:
            messagebox.showerror("Fehler", "pyodbc nicht installiert.\npip install pyodbc")
            return
        mdf = self.mdf_path.get().strip()
        if not mdf or not os.path.isfile(mdf):
            messagebox.showerror("Fehler", "Bitte eine gültige .mdf-Datei auswählen.")
            return
        driver = self.driver_var.get()
        if not driver:
            messagebox.showerror("Fehler", "Kein ODBC-Treiber ausgewählt.")
            return

        def task():
            self.after(0, self._set_led, self._led_schema, "running")
            self.after(0, self._progress_start_indeterminate, "Schema lesen …")
            session = None
            try:
                self.log(f"── Schema lesen: {mdf}")
                self.log("Original-Datei wird nicht verändert – Tool arbeitet auf Kopie.")
                session      = attach_mdf(mdf, self.db_attach_name.get(), driver, self.log)
                self._schema = read_schema(session, self.log)
                self.log("Schema erfolgreich gelesen. → DDL generieren klicken.")
                self.after(0, self._set_led, self._led_schema, "ok")
                self.after(0, self._progress_finish, "Schema gelesen ✓", True, 3000)
            except Exception as e:
                self.log(f"FEHLER: {e}")
                self.after(0, self._set_led, self._led_schema, "error")
                self.after(0, self._progress_finish, f"Fehler: {e}", False, 0)
                self.after(0, messagebox.showerror, "Fehler", str(e))
            finally:
                if session is not None:
                    detach_and_cleanup(session, self.log)

        threading.Thread(target=task, daemon=True).start()

    def _generate_ddl(self):
        if not hasattr(self, "_schema"):
            messagebox.showinfo("Hinweis", "Bitte zuerst 'Schema lesen' ausführen.")
            return
        self._set_led(self._led_ddl, "running")
        self._progress_start_determinate("DDL generieren …")
        try:
            target_db = self.mysql_db.get().strip() or "migrated_db"
            self.log(f"Generiere DDL für Zieldatenbank '{target_db}' …")
            ddl = generate_mysql_ddl(self._schema, target_db)
            self.ddl_text.delete("1.0", "end")
            self.ddl_text.insert("1.0", ddl)
            tcount = len(self._schema["tables"])
            vcount = len(self._schema["views"])
            self.log(f"DDL generiert: {tcount} Tabellen, {vcount} Views. Prüfe Tab '3 · DDL-Vorschau'.")
            self._set_led(self._led_ddl, "ok")
            self._progress_finish(f"DDL: {tcount} Tabellen, {vcount} Views ✓", True, 3000)
        except Exception as e:
            self.log(f"FEHLER DDL: {e}")
            self._set_led(self._led_ddl, "error")
            self._progress_finish(f"Fehler: {e}", False, 0)

    def _save_ddl(self):
        ddl = self.ddl_text.get("1.0", "end").strip()
        if not ddl:
            messagebox.showinfo("Hinweis", "DDL-Vorschau ist leer.")
            return
        path = filedialog.asksaveasfilename(
            title="DDL speichern",
            defaultextension=".sql",
            initialdir=TEMP_DIR,
            filetypes=[("SQL-Datei", "*.sql"), ("Alle Dateien", "*.*")],
        )
        if path:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(ddl)
            self.log(f"DDL gespeichert: {path}")

    def _test_mysql(self):
        if not MYSQL_OK:
            messagebox.showerror(
                "Fehler",
                "mysql-connector-python nicht installiert.\npip install mysql-connector-python",
            )
            return
        try:
            conn = mysql.connector.connect(
                host=self.mysql_host.get().strip(),
                port=int(self.mysql_port.get().strip()),
                user=self.mysql_user.get().strip(),
                password=self.mysql_pass.get(),
                connection_timeout=5,
            )
            conn.close()
            self.log("✓ MySQL-Verbindung erfolgreich.")
            messagebox.showinfo("Verbindung OK", "MySQL-Verbindung erfolgreich!")
        except Exception as e:
            self.log(f"Verbindungsfehler: {e}")
            messagebox.showerror("Verbindungsfehler", str(e))

    def _deploy(self, resume: bool = False):
        if not MYSQL_OK:
            messagebox.showerror(
                "Fehler",
                "mysql-connector-python nicht installiert.\npip install mysql-connector-python",
            )
            return

        dry_run     = self._dry_run_var.get()
        incremental = self._incremental_var.get()
        data_scope  = self._data_scope_var.get()   # "all" oder "diff"

        # Inkrementell: Schema aus MDF erforderlich
        if incremental and not hasattr(self, "_schema"):
            messagebox.showinfo(
                "Hinweis",
                "Schema-Diff-Modus: Bitte zuerst 'Schema lesen' ausführen.",
            )
            return

        # Vollständig: DDL-Tab muss befüllt sein
        if not incremental:
            ddl = self.ddl_text.get("1.0", "end").strip()
            if not ddl:
                messagebox.showinfo("Hinweis", "DDL-Vorschau ist leer. Bitte zuerst DDL generieren.")
                return

        if not dry_run and not resume and not incremental:
            if not messagebox.askyesno(
                "Vollständiges Deployment bestätigen",
                f"ACHTUNG: DROP TABLE + CREATE TABLE auf\n"
                f"{self.mysql_host.get()}:{self.mysql_port.get()} / {self.mysql_db.get()}\n\n"
                f"Alle bestehenden Daten in der Zieldatenbank werden gelöscht!\n\n"
                f"Jetzt ausführen?",
            ):
                return
        elif not dry_run and not resume and incremental:
            if not messagebox.askyesno(
                "Schema-Diff deployen",
                f"Inkrementelles Schema-Update auf\n"
                f"{self.mysql_host.get()}:{self.mysql_port.get()} / {self.mysql_db.get()}\n\n"
                f"Bestehende Daten bleiben erhalten.\n"
                f"Nur neue Tabellen/Spalten/Indexes werden angelegt.\n\n"
                f"Jetzt ausführen?",
            ):
                return

        host      = self.mysql_host.get().strip()
        port      = int(self.mysql_port.get().strip())
        user      = self.mysql_user.get().strip()
        password  = self.mysql_pass.get()
        target_db = self.mysql_db.get().strip()

        def task():
            # diff_result wird ggf. in Schritt 1 befüllt und in Schritt 2 genutzt
            diff_result = None

            # ══════════════════════════════════════════════════════════════
            # Schritt 1: Schema-DDL deployen
            # ══════════════════════════════════════════════════════════════
            if not resume:
                self.after(0, self._set_led, self._led_deploy, "running")

                if incremental:
                    # ── Inkrementell: Diff berechnen und deployen ─────────
                    self.after(0, self._progress_start_indeterminate, "Schema-Diff berechnen …")
                    try:
                        mysql_conn_schema = mysql.connector.connect(
                            host=host, port=port, user=user, password=password,
                            database=target_db, charset="utf8mb4", connection_timeout=10,
                        )
                        self.log("── Schema-Diff: Lese MySQL-Schema …")
                        mysql_schema = read_mysql_schema(mysql_conn_schema, target_db)
                        mysql_conn_schema.close()

                        diff_result = diff_schemas(self._schema, mysql_schema)
                        summary = format_diff_summary(diff_result)
                        self.log("── Schema-Diff Ergebnis:")
                        self.log(summary)

                        if diff_result["warnings"]:
                            self.log("⚠ Warnungen:")
                            for w in diff_result["warnings"]:
                                self.log(f"  {w}")

                        # Kein Änderungsbedarf?
                        no_changes = (
                            not diff_result["new_tables"]
                            and not diff_result["altered_tables"]
                        )
                        if no_changes:
                            self.log("✓ Schema ist bereits aktuell – kein Deploy nötig.")
                            self.after(0, self._set_led, self._led_deploy, "ok")
                            self.after(0, self._progress_finish, "Schema aktuell ✓", True, 3000)
                            if dry_run or not self._transfer_data_var.get():
                                return
                        elif dry_run:
                            # Dry-Run: Diff + Datenvorschau anzeigen
                            if self._transfer_data_var.get():
                                refresh_set = get_tables_to_refresh(diff_result)
                                if data_scope == "diff":
                                    self.log(f"── Dry-Run Daten: {len(refresh_set)} Tabellen würden neu geladen:")
                                    for t in sorted(refresh_set):
                                        self.log(f"  → {t}")
                                else:
                                    self.log("── Dry-Run Daten: alle Tabellen würden neu geladen (Alle-Modus)")
                            self.log("✓ Dry-Run: Schema-Diff angezeigt – keine Änderungen ausgeführt.")
                            self.after(0, self._set_led, self._led_deploy, "ok")
                            self.after(0, self._progress_finish, "Dry-Run: Diff OK ✓", True, 3000)
                            return
                        else:
                            diff_ddl, _ = generate_diff_ddl(diff_result, self._schema, target_db)
                            self.after(0, self._show_diff_ddl, diff_ddl)

                            self.after(0, self._progress_start_determinate, "Schema-Diff deployen …")
                            deploy_to_mysql(
                                diff_ddl,
                                host=host, port=port, user=user,
                                password=password, target_db=target_db,
                                log=self.log,
                                progress_callback=self._deploy_progress_callback,
                            )
                            self.log("✓ Schema-Diff erfolgreich deployt – Daten erhalten.")
                            self.after(0, self._set_led, self._led_deploy, "ok")
                            self.after(0, self._progress_finish, "Schema-Diff deployed ✓", True, 3000)

                    except Exception as e:
                        self.log(f"FEHLER Schema-Diff: {e}")
                        self.after(0, self._set_led, self._led_deploy, "error")
                        self.after(0, self._progress_finish, f"Fehler: {e}", False, 0)
                        self.after(0, messagebox.showerror, "Fehler Schema-Diff", str(e))
                        return

                else:
                    # ── Vollständig: DROP + CREATE (bisheriges Verhalten) ─
                    if dry_run:
                        self.log("ℹ Dry-Run + Vollständig: DDL-Vorschau im Tab '3 · DDL-Vorschau'.")
                        self.after(0, self._set_led, self._led_deploy, "ok")
                        self.after(0, self._progress_finish, "Dry-Run: DDL bereit ✓", True, 3000)
                        if not self._transfer_data_var.get():
                            return
                    else:
                        ddl = self.ddl_text.get("1.0", "end").strip()
                        self.after(0, self._progress_start_determinate, "DDL deployen …")
                        try:
                            deploy_to_mysql(
                                ddl,
                                host=host, port=port, user=user,
                                password=password, target_db=target_db,
                                log=self.log,
                                progress_callback=self._deploy_progress_callback,
                            )
                            self.after(0, self._set_led, self._led_deploy, "ok")
                            self.after(0, self._progress_finish, "DDL deployed ✓", True, 3000)
                        except Exception as e:
                            self.log(f"FEHLER beim Deployment: {e}")
                            self.after(0, self._set_led, self._led_deploy, "error")
                            self.after(0, self._progress_finish, f"Deploy-Fehler: {e}", False, 0)
                            self.after(0, messagebox.showerror, "Fehler", str(e))
                            return

            # ══════════════════════════════════════════════════════════════
            # Schritt 2: Daten übertragen (optional)
            # ══════════════════════════════════════════════════════════════
            if not self._transfer_data_var.get() and not resume:
                return

            self.log("")
            mode_label = "Dry-Run Vorschau" if dry_run else ("Resume" if resume else "Daten übertragen")
            self.log(f"── {mode_label}")
            mdf = self.mdf_path.get().strip()
            if not mdf or not os.path.isfile(mdf):
                self.log("⚠ Datenmigration übersprungen: keine gültige .mdf-Datei angegeben.")
                return

            self._stop_event.clear()
            self.after(0, self._set_migration_running, True)
            self.after(0, self._progress_start_determinate, "Datenmigration startet …")

            session = None
            try:
                session = attach_mdf(
                    mdf, self.db_attach_name.get(),
                    self.driver_var.get(), self.log,
                )
                tables     = get_table_list(session)
                mysql_conn = mysql.connector.connect(
                    host=host, port=port, user=user, password=password,
                    database=target_db, charset="utf8mb4", connection_timeout=10,
                )
                # Whitelist bestimmen
                tables_whitelist = None
                if data_scope == "diff" and not resume:
                    if diff_result is None and incremental and hasattr(self, "_schema"):
                        # Diff noch nicht berechnet (z.B. Schema bereits aktuell)
                        try:
                            mc_tmp = mysql.connector.connect(
                                host=host, port=port, user=user, password=password,
                                database=target_db, charset="utf8mb4", connection_timeout=10,
                            )
                            diff_result = diff_schemas(
                                self._schema, read_mysql_schema(mc_tmp, target_db)
                            )
                            mc_tmp.close()
                        except Exception:
                            pass
                    if diff_result is not None:
                        tables_whitelist = get_tables_to_refresh(diff_result)
                        self.log(f"  Daten-Scope: Nur geänderte Tabellen ({len(tables_whitelist)})")
                        for t in sorted(tables_whitelist):
                            self.log(f"    → {t}")
                    else:
                        self.log("  Daten-Scope: Diff nicht verfügbar – alle Tabellen werden migriert.")
                else:
                    if not resume:
                        self.log("  Daten-Scope: Alle Tabellen werden neu befüllt.")

                self.after(0, self._set_led, self._led_deploy, "running")
                self.after(0, self._progress_start_determinate, "Daten migrieren …")
                result = migrate_all(
                    session, mysql_conn, tables, self.log,
                    chunk_size=CHUNK_SIZE,
                    progress_callback=self._progress_callback,
                    stop_event=self._stop_event,
                    dry_run=dry_run,
                    checkpoint_file=CHECKPOINT_FILE,
                    tables_whitelist=tables_whitelist,
                )
                mysql_conn.close()
                if dry_run:
                    self.log("✓ Dry-Run abgeschlossen – keine Daten geschrieben.")
                    self.after(0, self._set_led, self._led_deploy, "ok")
                    self.after(0, self._progress_finish, "Dry-Run abgeschlossen ✓", True, 3000)
                elif result["cancelled"]:
                    self.log(f"⚠ Migration abgebrochen: {result['total_rows']:,} Zeilen importiert.")
                    self.after(0, self._set_led, self._led_deploy, "error")
                    self.after(0, self._progress_finish,
                               f"Abgebrochen – {result['total_rows']:,} Zeilen", False, 0)
                elif result["errors"]:
                    self.log(f"✓ Datenmigration: {result['total_rows']:,} Zeilen, {len(result['errors'])} Fehler.")
                    self.after(0, self._set_led, self._led_deploy, "error")
                    self.after(0, self._progress_finish,
                               f"{result['total_rows']:,} Zeilen – {len(result['errors'])} Fehler", False, 0)
                    for err in result["errors"]:
                        self.log(f"  ⚠ {err}")
                else:
                    self.log(f"✓ Datenmigration abgeschlossen: {result['total_rows']:,} Zeilen importiert.")
                    self.after(0, self._set_led, self._led_deploy, "ok")
                    self.after(0, self._progress_finish,
                               f"Migration: {result['total_rows']:,} Zeilen ✓", True, 3000)
                self.after(0, self._refresh_resume_btn)
            except Exception as e:
                self.log(f"FEHLER Datenmigration: {e}")
                self.after(0, self._set_led, self._led_deploy, "error")
                self.after(0, self._progress_finish, f"Fehler: {e}", False, 0)
                self.after(0, messagebox.showerror, "Fehler Datenmigration", str(e))
            finally:
                if session is not None:
                    detach_and_cleanup(session, self.log)
                self.after(0, self._set_migration_running, False)

        threading.Thread(target=task, daemon=True).start()

    def _show_diff_ddl(self, ddl: str):
        """Zeigt das Diff-DDL im DDL-Tab an (thread-safe via after())."""
        self.ddl_text.delete("1.0", "end")
        self.ddl_text.insert("1.0", ddl)

    # ── Konfiguration ────────────────────────────────────────────────────
    def _all_profiles(self) -> dict:
        if os.path.isfile(CFG_FILE):
            try:
                with open(CFG_FILE, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _refresh_profiles(self):
        profiles = list(self._all_profiles().keys())
        self._profile_combo["values"] = profiles or ["Standard"]
        if not self._profile_var.get() and profiles:
            self._profile_var.set(profiles[0])

    def _save_config(self):
        profile = self._profile_var.get().strip() or "Standard"
        pw_obf  = base64.b64encode(self.mysql_pass.get().encode()).decode()
        data    = {
            "mdf_path":       self.mdf_path.get(),
            "db_attach_name": self.db_attach_name.get(),
            "driver":         self.driver_var.get(),
            "mysql_host":     self.mysql_host.get(),
            "mysql_port":     self.mysql_port.get(),
            "mysql_user":     self.mysql_user.get(),
            "mysql_pass_b64": pw_obf,
            "mysql_db":       self.mysql_db.get(),
            "transfer_data":  self._transfer_data_var.get(),
            "saved_at":       datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        all_cfg          = self._all_profiles()
        all_cfg[profile] = data
        with open(CFG_FILE, "w", encoding="utf-8") as fh:
            json.dump(all_cfg, fh, ensure_ascii=False, indent=2)
        self._refresh_profiles()
        self._profile_var.set(profile)
        msg = f"✓ Profil '{profile}' gespeichert."
        self._cfg_status.config(text=msg, foreground="#006600")
        self.log(msg)
        self.after(3000, lambda: self._cfg_status.config(text=""))

    def _load_config(self, profile: Optional[str] = None):
        all_cfg = self._all_profiles()
        if not all_cfg:
            return
        if profile is None:
            profile = self._profile_var.get().strip()
            if profile not in all_cfg:
                profile = next(iter(all_cfg))
        if profile not in all_cfg:
            self.log(f"⚠ Profil '{profile}' nicht gefunden.")
            return
        d = all_cfg[profile]
        self.mdf_path.set(       d.get("mdf_path",       ""))
        self.db_attach_name.set( d.get("db_attach_name", "MigratedDB"))
        self.mysql_host.set(     d.get("mysql_host",     ""))
        self.mysql_port.set(     d.get("mysql_port",     "3306"))
        self.mysql_user.set(     d.get("mysql_user",     ""))
        self.mysql_db.set(       d.get("mysql_db",       ""))
        try:
            pw = base64.b64decode(d.get("mysql_pass_b64", "")).decode()
        except Exception:
            pw = ""
        self.mysql_pass.set(pw)
        self._transfer_data_var.set(d.get("transfer_data", False))
        saved_driver = d.get("driver", "")
        if saved_driver:
            self.driver_var.set(saved_driver)
        self._profile_var.set(profile)
        self._refresh_profiles()
        ts  = d.get("saved_at", "")
        msg = f"✓ Profil '{profile}' geladen  (gespeichert: {ts})"
        self._cfg_status.config(text=f"Profil '{profile}' geladen", foreground="#006600")
        self.log(msg)
        self.after(4000, lambda: self._cfg_status.config(text=""))

    def _delete_profile(self):
        profile = self._profile_var.get().strip()
        if not profile:
            return
        if not messagebox.askyesno("Profil löschen", f"Profil '{profile}' wirklich löschen?"):
            return
        all_cfg = self._all_profiles()
        if profile in all_cfg:
            del all_cfg[profile]
            with open(CFG_FILE, "w", encoding="utf-8") as fh:
                json.dump(all_cfg, fh, ensure_ascii=False, indent=2)
            self.log(f"Profil '{profile}' gelöscht.")
        self._refresh_profiles()
        if all_cfg:
            self._profile_var.set(next(iter(all_cfg)))
        else:
            self._profile_var.set("Standard")


# Import delayed to avoid circular dependency when ui.py is imported standalone
from mssql import read_schema  # noqa: E402  (used in _read_schema task())

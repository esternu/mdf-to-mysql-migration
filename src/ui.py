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
from paths        import CFG_FILE, LOG_FILE, TEMP_DIR
from mssql        import attach_mdf, detach_and_cleanup, get_mssql_drivers, PYODBC_OK
from transform    import generate_mysql_ddl
from deploy       import deploy_to_mysql, MYSQL_OK
from migrate_data import get_table_list, migrate_all

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
        self.geometry("820x700")
        self.resizable(True, True)
        self._build_ui()
        self._load_config()
        self._check_deps()

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

        # Aktions-Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", padx=8, pady=(0, 4))
        ttk.Button(btn_frame, text="Schema lesen",          command=self._read_schema).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="DDL generieren",        command=self._generate_ddl).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="DDL speichern …",       command=self._save_ddl).pack(side="left", padx=4)

        # Checkbox: Daten nach dem Schema-Deploy übertragen
        self._transfer_data_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            btn_frame,
            text="Daten übertragen",
            variable=self._transfer_data_var,
        ).pack(side="left", padx=(12, 2))

        ttk.Button(btn_frame, text="▶ Auf MySQL deployen",  command=self._deploy).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Abhängigkeiten prüfen", command=self._check_deps).pack(side="right", padx=4)

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
        ttk.Label(f, text=".mdf Datei:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.mdf_path = tk.StringVar()
        ttk.Entry(f, textvariable=self.mdf_path, width=55).grid(row=0, column=1, padx=4, pady=6)
        ttk.Button(f, text="Durchsuchen …", command=self._browse_mdf).grid(row=0, column=2, padx=4)

        ttk.Label(f, text="Datenbank-Name (intern):").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.db_attach_name = tk.StringVar(value="MigratedDB")
        ttk.Entry(f, textvariable=self.db_attach_name, width=30).grid(row=1, column=1, sticky="w", padx=4)

        ttk.Label(f, text="ODBC-Treiber:").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        self.driver_var   = tk.StringVar()
        self.driver_combo = ttk.Combobox(f, textvariable=self.driver_var, width=52)
        self.driver_combo.grid(row=2, column=1, padx=4, pady=6)
        ttk.Button(f, text="Treiber aktualisieren", command=self._refresh_drivers).grid(row=2, column=2, padx=4)

        info = (
            "Hinweis: Zum Lesen der .mdf-Datei wird Microsoft SQL Server LocalDB\n"
            "oder SQL Server Express benötigt (kostenlos bei Microsoft erhältlich).\n"
            "Installer: https://aka.ms/sqllocaldb\n\n"
            "Alternativ: DDL-Datei manuell aus SQL Server Management Studio exportieren\n"
            "und im Tab '3 · DDL-Vorschau' einfügen."
        )
        ttk.Label(f, text=info, foreground="#555", justify="left").grid(
            row=3, column=0, columnspan=3, padx=8, pady=12, sticky="w")
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
            session = None
            try:
                self.log(f"── Schema lesen: {mdf}")
                self.log("Original-Datei wird nicht verändert – Tool arbeitet auf Kopie.")
                session      = attach_mdf(mdf, self.db_attach_name.get(), driver, self.log)
                self._schema = read_schema(session, self.log)
                self.log("Schema erfolgreich gelesen. → DDL generieren klicken.")
            except Exception as e:
                self.log(f"FEHLER: {e}")
                messagebox.showerror("Fehler", str(e))
            finally:
                if session is not None:
                    detach_and_cleanup(session, self.log)

        threading.Thread(target=task, daemon=True).start()

    def _generate_ddl(self):
        if not hasattr(self, "_schema"):
            messagebox.showinfo("Hinweis", "Bitte zuerst 'Schema lesen' ausführen.")
            return
        target_db = self.mysql_db.get().strip() or "migrated_db"
        self.log(f"Generiere DDL für Zieldatenbank '{target_db}' …")
        ddl = generate_mysql_ddl(self._schema, target_db)
        self.ddl_text.delete("1.0", "end")
        self.ddl_text.insert("1.0", ddl)
        tcount = len(self._schema["tables"])
        vcount = len(self._schema["views"])
        self.log(f"DDL generiert: {tcount} Tabellen, {vcount} Views. Prüfe Tab '3 · DDL-Vorschau'.")

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

    def _deploy(self):
        if not MYSQL_OK:
            messagebox.showerror(
                "Fehler",
                "mysql-connector-python nicht installiert.\npip install mysql-connector-python",
            )
            return
        ddl = self.ddl_text.get("1.0", "end").strip()
        if not ddl:
            messagebox.showinfo("Hinweis", "DDL-Vorschau ist leer. Bitte zuerst DDL generieren.")
            return
        if not messagebox.askyesno(
            "Deployment bestätigen",
            f"DDL auf {self.mysql_host.get()}:{self.mysql_port.get()}\n"
            f"Datenbank: {self.mysql_db.get()}\n\nJetzt ausführen?",
        ):
            return

        host     = self.mysql_host.get().strip()
        port     = int(self.mysql_port.get().strip())
        user     = self.mysql_user.get().strip()
        password = self.mysql_pass.get()
        target_db = self.mysql_db.get().strip()

        def task():
            # ── Schritt 1: DDL deployen ──────────────────────────────────
            try:
                deploy_to_mysql(
                    ddl,
                    host=host, port=port, user=user,
                    password=password, target_db=target_db,
                    log=self.log,
                )
            except Exception as e:
                self.log(f"FEHLER beim Deployment: {e}")
                messagebox.showerror("Fehler", str(e))
                return

            # ── Schritt 2: Daten übertragen (optional) ───────────────────
            if not self._transfer_data_var.get():
                return

            self.log("")
            self.log("── Daten übertragen")
            mdf = self.mdf_path.get().strip()
            if not mdf or not os.path.isfile(mdf):
                self.log("⚠ Datenmigration übersprungen: keine gültige .mdf-Datei angegeben.")
                return

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
                result = migrate_all(session, mysql_conn, tables, self.log)
                mysql_conn.close()
                self.log(f"✓ Datenmigration abgeschlossen: {result['total_rows']} Zeilen importiert.")
                if result["errors"]:
                    for err in result["errors"]:
                        self.log(f"  ⚠ {err}")
            except Exception as e:
                self.log(f"FEHLER Datenmigration: {e}")
                messagebox.showerror("Fehler Datenmigration", str(e))
            finally:
                if session is not None:
                    detach_and_cleanup(session, self.log)

        threading.Thread(target=task, daemon=True).start()

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

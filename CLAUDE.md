# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A tool that migrates SQL Server `.mdf` database files to a MySQL/MariaDB server
(typically a Synology NAS). It reads the SQL Server schema (tables, columns, PKs,
FKs, indexes, views), translates SQL Server types and T-SQL view definitions into
MySQL DDL, deploys that DDL, and optionally copies the table data row-by-row.

Note: the README and most code comments/log messages are in **German**. New
strings and comments should match this convention. The README's "Codestruktur"
section is slightly out of date — it omits `migrate_data.py` / `run_migrate_data.py`
and the checkpoint feature documented below.

## Commands

```bash
# Run the full test suite (pyodbc-dependent tests need unixodbc-dev installed first)
pytest tests/ -v --tb=short

# Run a single test file / class / test
pytest tests/test_transform.py
pytest tests/test_transform.py::TestConvertType
pytest tests/test_transform.py::TestConvertType::test_money

# Install dependencies (pyodbc requires the system lib below on Linux)
sudo apt-get install -y unixodbc-dev   # needed to compile pyodbc
pip install -r requirements.txt

# GUI entry point (Windows; needs tkinter + SQL Server LocalDB)
py mdf_to_mysql.py            # or: start_tool.bat

# Headless schema → DDL → deploy (reads first profile of config.json)
py run_headless.py

# Headless data migration (separate step from schema/DDL)
py run_migrate_data.py            # full data copy
py run_migrate_data.py --dry-run  # preview row counts, write nothing
py run_migrate_data.py --resume   # continue from checkpoint after a crash

# Docker (Linux; requires a network-reachable SQL Server, not LocalDB)
docker build -t mdf-to-mysql .
docker run --rm -v $(pwd)/config.json:/app/config.json:ro -v $(pwd)/output:/app/temp \
  -e MSSQL_SERVER=host -e MSSQL_USER=sa -e MSSQL_PASS=pw mdf-to-mysql
```

CI (`.github/workflows/ci.yml`) runs `pytest` on Python 3.10/3.11/3.12, then a
Docker build that only runs if tests pass.

## Architecture

The codebase is a thin set of entry points wrapping pure, testable modules in
`src/`. Entry points handle orchestration (config loading, connection setup,
file logging); `src/` modules contain the logic.

**Two independent migration paths** — schema and data are deliberately separate:

1. **Schema/DDL path**: `attach_mdf` → `read_schema` → `generate_mysql_ddl` →
   `deploy_to_mysql`. Driven by the GUI (Tab 1–3) or `run_headless.py`.
2. **Data path**: `attach_mdf` → `get_table_list` → `migrate_all`. Driven by the
   GUI or `run_migrate_data.py`. This `TRUNCATE`s each target table then bulk-inserts.

Module layout:

- `src/paths.py` — single source of truth for all paths. Note `LOG_DIR` is
  **outside** the project (`../mdf-to-mysql-logs/`); `CHECKPOINT_FILE` and
  generated SQL live in `temp/`. Importing this module creates these dirs.
- `src/mssql.py` — SQL Server access via pyodbc. `attach_mdf` always works on a
  **temporary copy** of the `.mdf` (the original is never modified) and detaches +
  deletes the copy in `detach_and_cleanup`. `_build_conn_str` switches between
  Windows-auth LocalDB (default) and SQL-auth via the `MSSQL_SERVER` / `MSSQL_USER`
  / `MSSQL_PASS` env vars (used by Docker/Linux). `read_schema` returns a dict with
  `tables` and `views` keys — this dict shape is the contract consumed by `transform.py`.
- `src/transform.py` — pure SQL Server → MySQL conversion (no DB/IO). `TYPE_MAP` +
  `convert_type` for types, `convert_view_sql` for T-SQL view bodies (regex-based:
  `STRING_AGG`→`GROUP_CONCAT`, `OUTER/CROSS APPLY`→`LEFT JOIN`/`JOIN`, string `+`
  concat→`CONCAT`, `ISNULL`/`IIF`/`LEN`/`GETDATE` etc.), and `generate_mysql_ddl`
  which assembles everything. Views are emitted in dependency order via
  `_topo_sort_views` (Kahn's algorithm; cycles get appended as-is).
- `src/deploy.py` — splits DDL on `;` and executes statement-by-statement,
  **collecting errors instead of aborting** so a bad statement doesn't stop the rest.
- `src/migrate_data.py` — chunked data copy (`CHUNK_SIZE = 5000`). Pure functions
  (`get_table_list`, `get_row_counts`, `iter_table_data`, `migrate_table`,
  `migrate_all`). Disables `FOREIGN_KEY_CHECKS` during load. Has a JSON **checkpoint**
  system (`load/save/delete_checkpoint`) so `--resume` skips already-completed tables;
  checkpoint is deleted on clean completion. Reads source via `OFFSET/FETCH` paging;
  converts `memoryview` (binary) values to `bytes` for the MySQL driver.
- `src/ui.py` — tkinter `App` (4 tabs: source / target / DDL preview / log). Long
  operations run on background threads with `stop_event` cancellation and progress
  callbacks. **`ui.py` is never imported by tests** (`conftest.py` notes it needs a
  display); keep GUI-free logic in the other modules so it stays testable.

Dependency direction: `run_*` and `ui.py` import from `src/`; `src/` modules import
each other only as shown above (`transform` and `deploy` have no SQL Server deps).

## Conventions & Gotchas

- **Optional dependency guards**: `pyodbc` and `mysql.connector` are imported in
  `try/except` with `*_OK` flags so the pure modules import even when the driver is
  absent. Tests rely on this; preserve the pattern when adding imports.
- **`sys.path` shim**: entry points and `conftest.py` insert `src/` onto `sys.path`,
  so modules import each other by bare name (`from transform import ...`, not
  `from src.transform import ...`). Match this in new test/entry code.
- **`config.json`** is git-ignored and holds connection profiles keyed by profile
  name. Headless runners use `next(iter(all_cfg))` — the **first** profile. The MySQL
  password is stored base64-encoded under `mysql_pass_b64` (obfuscation, *not*
  encryption). Profile keys: `mdf_path`, `db_attach_name`, `driver`, `mysql_host`,
  `mysql_port`, `mysql_user`, `mysql_pass_b64`, `mysql_db`, `transfer_data`.
- `.mdf`/`.ldf`/`.sql.bak`, `*.log`, and `temp/*.sql` are git-ignored — never commit
  customer data, credentials, or generated artifacts.
- **Testing approach**: tests mock DB cursors/connections (`pytest-mock`); they do
  not require a live SQL Server or MySQL. `transform` and `migrate_data` logic is the
  most heavily covered. The 4 `test_mssql.py::TestGetMssqlDrivers` tests patch
  `pyodbc.drivers`, so they fail unless `pyodbc` is importable (install `unixodbc-dev`
  + `pyodbc` first) — this is an environment issue, not a code regression.

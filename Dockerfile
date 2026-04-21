# ═══════════════════════════════════════════════════════════════════════════
#  MDF → MySQL Migration Tool – Docker Image
#
#  Läuft auf Linux; benötigt einen SQL Server auf dem Netzwerk (kein LocalDB).
#  Konfiguration via Umgebungsvariablen oder gemounteter config.json.
#
#  Umgebungsvariablen:
#    MSSQL_SERVER  – SQL Server-Adresse, z.B. "sqlserver:1433"
#    MSSQL_USER    – SQL Server-Benutzername (SQL-Authentifizierung)
#    MSSQL_PASS    – SQL Server-Passwort
#
#  Volumes (empfohlen):
#    /app/config.json   – Verbindungskonfiguration (Profil 'Standard')
#    /app/temp/         – Ausgabeverzeichnis für generiertes DDL
#
#  Beispiel:
#    docker run --rm \
#      -v $(pwd)/config.json:/app/config.json:ro \
#      -v $(pwd)/output:/app/temp \
#      -e MSSQL_SERVER=192.168.1.50 \
#      -e MSSQL_USER=sa \
#      -e MSSQL_PASS=MyPassword \
#      mdf-to-mysql
# ═══════════════════════════════════════════════════════════════════════════

FROM python:3.11-slim

WORKDIR /app

# ── System-Abhängigkeiten ─────────────────────────────────────────────────
# unixodbc-dev  → Kompilierung von pyodbc
# msodbcsql18   → Microsoft ODBC Driver 18 for SQL Server (Linux)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        apt-transport-https \
        ca-certificates \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
        https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Python-Abhängigkeiten ─────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir pyodbc mysql-connector-python

# ── Quellcode ─────────────────────────────────────────────────────────────
COPY src/            ./src/
COPY run_headless.py .

# Ausgabe-Verzeichnisse anlegen
RUN mkdir -p temp

# ── Umgebungsvariablen ────────────────────────────────────────────────────
# Standard leer → _build_conn_str fällt auf LocalDB-Syntax zurück.
# Auf Linux muss MSSQL_SERVER auf einen erreichbaren SQL Server zeigen.
ENV MSSQL_SERVER=""
ENV MSSQL_USER=""
ENV MSSQL_PASS=""

# ── Einstiegspunkt ────────────────────────────────────────────────────────
ENTRYPOINT ["python", "run_headless.py"]

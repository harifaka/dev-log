# dev-log

An enterprise-grade, lightweight hybrid log investigator and root cause analysis
(RCA) utility. It will map asynchronous transaction lifecycles across
microservices from a single business identifier, combining Oracle/MSSQL logs,
Graylog, and RabbitMQ streams.

## Quick start

Requires Python 3.11 or newer (the automated Windows build uses Python 3.12).

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python app.py
```

The application opens `http://127.0.0.1:5050/` automatically. Copy
`config.json` beside a packaged executable and edit that external file; never
commit real credentials or API tokens. A malformed configuration is reported
in the dashboard and through `/api/health`, rather than preventing the shell
from starting.

## Connector API

Enable connectors in the active environment of `config.json`. Database profiles
are restricted to bound, single-statement `SELECT` queries and use five-second
operation limits. Oracle uses the `oracledb` Thin Mode pool; SQL Server uses
`pyodbc`; Graylog uses `API_TOKEN:session` Basic Auth; and RabbitMQ samples
messages with requeue enabled so the queue is not consumed.

- `GET /api/schema/reflect?service=orders` reflects columns for a relational profile.
- `POST /api/trace/execute` accepts `{"business_id":"...", "correlation_id":"..."}`.

Connector failures become warning events in the response, allowing available
services to return partial timelines.

## Project map

| Path | Purpose |
| --- | --- |
| `app.py` | Flask application factory and Waitress lifecycle |
| `config.json` | External environment and connector placeholders |
| `query_templates.json` | Read-only, parameterized service query profiles |
| `templates/index.html` | Self-contained offline dashboard shell |
| `.github/workflows/release.yml` | Reproducible Windows `.exe` one-file build on every `main` push |
| `ROADMAP.md` | Six-phase delivery plan |
| `RELEASE_NOTES.md` | Change history |

## Portable Windows build

The GitHub Actions workflow installs the Python dependencies, runs PyInstaller,
and creates a uniquely tagged GitHub release for each push to `main`. Locally:

```bash
pip install -r requirements.txt pyinstaller
pyinstaller --clean --noconfirm --onefile --windowed \
  --add-data "templates:templates" --name "dev-log" app.py
```

On Windows, use the required PyInstaller separator:

```powershell
pyinstaller --noconfirm --onefile --windowed --add-data "templates;templates" --name "dev-log" app.py
```

The resulting `dist/dev-log.exe` is a one-file executable. The HTML dashboard
embeds its offline network runtime and therefore makes no CDN requests. Runtime
configuration remains external so environments and credentials are not baked
into the binary.

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

## Template-driven identification and tracing

`query_templates.json` now declares an `identification_strategy` per service.
Each strategy defines:

- `id_type` — one of `strict_numeric`, `uuid`, `partial_match`, `context_id`, or `raw_token`.
- `validation_regex` — the pattern used to validate the search identifier before any query runs.
- `expected_format` — a human-readable label shown in the dashboard badge.
- `case_sensitive` — whether validation and correlation extraction are case-sensitive.
- `query` — the service-specific query template using safe placeholders such as
  `:business_id`, `@business_id`, `:search_id_like`, or Graylog-style `{business_id}`.
- `correlation_extractor_regex` — the regex used to pull a correlation token from
  raw text, XML, or JSON payloads for that service.

`config.json` adds two global controls:

- `allow_raw_regex_queries` — when `true`, advanced users may enter raw Regular
  Expressions directly into the search bar; the backend validates the regex syntax
  instead of the service-specific pattern.
- `fallback_correlation_regex` — used when a service strategy omits its own
  `correlation_extractor_regex`.

The dashboard exposes an **Ingress Service** dropdown and a **Search identifier**
input. Selecting a service fetches `/api/strategy/<service>` and updates the
expected-format badge and client-side validation in real time. Backend validation
runs before any connector query, returning a clear UI message instead of a
database exception.

## Connector API

Enable connectors in the active environment of `config.json`. Database profiles
are restricted to bound, single-statement `SELECT` queries and use five-second
operation limits. Oracle uses the `oracledb` Thin Mode pool; SQL Server uses
`pyodbc`; Graylog uses `API_TOKEN:session` Basic Auth; and RabbitMQ samples
messages with requeue enabled so the queue is not consumed.

- `GET /api/strategy/<service>` returns the identification strategy, expected format,
  validation regex, and raw-regex mode for the selected service.
- `GET /api/schema/reflect?service=orders` reflects columns for a relational profile.
- `POST /api/trace/execute` accepts `{"service":"...", "search_id":"...", "correlation_id":"..."}`.
  A legacy `business_id` payload is still accepted and mapped to the `gateway` service.

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

On Windows, use the required semicolon separator (`;`); Unix-like systems use
the colon separator (`:`):

```powershell
pyinstaller --noconfirm --onefile --windowed --add-data "templates;templates" --name "dev-log" app.py
```

The resulting `dist/dev-log.exe` is a one-file executable. The HTML dashboard
embeds its offline network runtime and therefore makes no CDN requests. Runtime
configuration remains external so environments and credentials are not baked
into the binary.

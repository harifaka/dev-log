# dev-log

An enterprise-grade, lightweight hybrid log investigator and root cause analysis
(RCA) utility. It will map asynchronous transaction lifecycles across
microservices from a single business identifier, combining Oracle/MSSQL logs,
Graylog, and RabbitMQ streams.

## Phase 1 quick start

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

## Project map

| Path | Purpose |
| --- | --- |
| `app.py` | Flask application factory and Waitress lifecycle |
| `config.json` | External environment and connector placeholders |
| `query_templates.json` | Read-only, parameterized service query profiles |
| `templates/index.html` | Self-contained offline dashboard shell |
| `dev-log.spec` | PyInstaller one-file build definition |
| `.github/workflows/release.yml` | Windows `.exe` release on every `main` push |
| `ROADMAP.md` | Six-phase delivery plan |
| `RELEASE_NOTES.md` | Change history |

## Portable Windows build

The GitHub Actions workflow installs the Python dependencies, runs PyInstaller,
and creates a uniquely tagged GitHub release for each push to `main`. Locally:

```bash
pip install -r requirements.txt pyinstaller
pyinstaller --clean --noconfirm dev-log.spec
```

The resulting `dist/dev-log.exe` is a one-file executable. Runtime
configuration remains external so environments and credentials are not baked
into the binary.

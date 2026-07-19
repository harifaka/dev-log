# dev-log roadmap

## Phase 1 — foundation (current)

- Local Flask application served by Waitress on `127.0.0.1:5050`.
- Browser launch hook, external environment configuration, and corruption-safe boot UI.
- Offline dashboard shell with an eight-service pipeline placeholder.
- PyInstaller specification and automated Windows release workflow.

## Phase 2 — relational adapters

- Add read-only Oracle Thin Mode pools and SQL Server 2022 adapters.
- Reflect allow-listed schema metadata and expose safe field projection controls.
- Enforce parameter binding and bounded query execution.

## Phase 3 — streams

- Add Graylog token authentication using `API_TOKEN:session`.
- Add RabbitMQ queue sampling, DLQ introspection, and bounded payload inspection.

## Phase 4 — correlation

- Resolve a business identifier to a global correlation ID.
- Query all enabled sources concurrently with cancellation and execution limits.
- Normalize and merge events into one chronological transaction timeline.

## Phase 5 — investigation experience

- Replace the graph placeholder with locally bundled Vis.js Network.
- Add node filtering, raw JSON/SQL console views, and failure state coloring.
- Generate formatted PDF action plans with ReportLab.

## Phase 6 — hardening

- Complete secret-provider integration, audit logging, and packaging verification.
- Add test coverage for configuration validation, parameter safety, and lifecycle behavior.

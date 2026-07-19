# dev-log roadmap

## Phase 1 — foundation (complete)

- Local Flask application served by Waitress on `127.0.0.1:5050`.
- Browser launch hook, external environment configuration, and corruption-safe boot UI.
- Offline dashboard shell with an eight-service pipeline placeholder.
- PyInstaller specification and automated Windows release workflow.

## Phase 2 — relational adapters (complete)

- Read-only Oracle Thin Mode pools and SQL Server 2022 adapters.
- Safe schema reflection through the `/api/schema/reflect` endpoint.
- Parameter binding, SELECT-only query sanitization, and five-second bounds.

## Phase 3 — streams (complete)

- Graylog Universal Search with `API_TOKEN:session` authentication.
- RabbitMQ queue depth and requeued DLQ sampling (maximum ten messages).
- Partial-failure warnings for unavailable infrastructure.

## Phase 4 — correlation (complete)

- Query configured service profiles concurrently from `/api/trace/execute`.
- Normalize and merge available events into one chronological timeline.
- Support per-request environment override; fault-isolated warnings injected into timeline.
- Frontend wired to backend: environment selector, business-ID form, live Investigate button.
- Service nodes colour-coded success/fault/skipped; click to filter event log by service.

## Phase 5 — investigation experience (partial)

- Node filtering and failure state colouring in the service pipeline panel.
- Raw chronological event console with service-level drill-down.
- `/api/report/pdf` endpoint: ReportLab-formatted RCA action plan with event timeline table.
- Frontend PDF download button; Vis.js network graph replacement still pending.

## Phase 6 — hardening

- Complete secret-provider integration, audit logging, and packaging verification.
- Add test coverage for configuration validation, parameter safety, and lifecycle behavior.

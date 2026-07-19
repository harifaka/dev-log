# Release notes

## Unreleased

### Added

- Phase 1 Flask/Waitress local application lifecycle.
- Loopback browser auto-launch and `/api/health` endpoint.
- Environment-aware `config.json` and eight-service `query_templates.json` blueprint.
- Offline dashboard HTML shell with service status placeholders.
- PyInstaller one-file specification and GitHub Actions Windows release workflow.
- Embedded offline Vis.js-compatible pipeline graph with status-driven node colours.
- Safe fallback profiles for missing or invalid runtime configuration.
- Trace-level `[System Fault Alert]` events preserving partial connector results.

### Changed

- Search, validation, and payload parsing are now fully template-driven through
  `query_templates.json` and `config.json`.
- `identification_strategy` per service replaces hardcoded business-ID validation
  and query construction; supports strict numeric keys, UUIDs, partial matches,
  context IDs, and raw tokens.
- Added dynamic expected-format badge and client-side validation in the dashboard
  that updates when the Ingress Service changes.
- Added `/api/strategy/<service>` endpoint to expose strategy metadata to the UI.

### Security

- The server binds only to `127.0.0.1`.
- Configuration contains placeholders only; credentials must be supplied outside source control.
- Query profiles document named, parameter-bound queries and do not permit SQL interpolation.
- Raw regex mode is opt-in via `allow_raw_regex_queries`; regex syntax is validated
  before any database interaction.

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

### Security

- The server binds only to `127.0.0.1`.
- Configuration contains placeholders only; credentials must be supplied outside source control.
- Query profiles document named, parameter-bound queries and do not permit SQL interpolation.

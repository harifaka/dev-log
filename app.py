"""dev-log desktop web application entry point.

The executable keeps configuration outside the binary so operators can change
connection profiles without rebuilding.  Only the loopback interface is
exposed by design; the application is an internal desktop utility, not a
network service.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template
from waitress import serve

LOGGER = logging.getLogger("dev-log")
HOST = "127.0.0.1"
PORT = 5050
# Empirically reliable minimum for Waitress to bind on slower desktop systems.
BROWSER_LAUNCH_DELAY_SECONDS = 0.75
# Four workers keep the local UI responsive without creating unbounded load.
WAITRESS_THREADS = 4
BASE_DIR = Path(__file__).resolve().parent


class ConfigurationError(RuntimeError):
    """Raised while parsing config.json or validating its operator settings.

    The application catches this error at startup and keeps the dashboard
    available so an operator can correct the external file without a traceback.
    """


@dataclass(frozen=True)
class RuntimeConfig:
    """Validated runtime settings needed by the Phase 1 shell."""

    active_environment: str
    server: dict[str, Any]
    environments: dict[str, Any]
    services: list[dict[str, Any]]


def resource_path(name: str) -> Path:
    """Resolve a bundled resource for source runs and PyInstaller one-files."""
    bundle_root = Path(getattr(sys, "_MEIPASS", BASE_DIR))
    external_root = Path.cwd()
    # The current working directory wins, allowing an executable's config to
    # be edited without unpacking or rebuilding it.
    external = external_root / name
    return external if external.exists() else bundle_root / name


def load_json(name: str) -> Any:
    """Load JSON with actionable errors instead of an opaque boot traceback."""
    path = resource_path(name)
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"Required file '{name}' was not found at {path}.") from exc
    except json.JSONDecodeError as exc:
        raise ConfigurationError(
            f"'{name}' is not valid JSON (line {exc.lineno}, column {exc.colno}): {exc.msg}."
        ) from exc
    except OSError as exc:
        raise ConfigurationError(f"Could not read '{name}': {exc}.") from exc


def load_runtime_config() -> RuntimeConfig:
    """Load and minimally validate the external environment configuration."""
    document = load_json("config.json")
    if not isinstance(document, dict):
        raise ConfigurationError("config.json must contain a JSON object.")
    active = document.get("active_environment")
    environments = document.get("environments")
    server = document.get("server", {})
    services = document.get("services")
    if not isinstance(active, str) or not isinstance(environments, dict) or not isinstance(server, dict):
        raise ConfigurationError(
            "config.json must define string 'active_environment', object 'server', and object 'environments'."
        )
    try:
        port = int(server.get("port", PORT))
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("config.json server.port must be an integer from 1 to 65535.") from exc
    if not 1 <= port <= 65535:
        raise ConfigurationError("config.json server.port must be an integer from 1 to 65535.")
    if active not in environments:
        raise ConfigurationError(
            f"Active environment '{active}' is not defined in config.json."
        )
    if not isinstance(services, list) or not all(isinstance(item, dict) for item in services):
        raise ConfigurationError("config.json 'services' must be an array of objects.")
    return RuntimeConfig(active, server, environments, services)


def create_app(configuration: RuntimeConfig | None = None) -> Flask:
    """Create the Flask application without starting a server."""
    app = Flask(
        __name__,
        template_folder=str(resource_path("templates")),
        static_folder=str(resource_path("static")),
    )
    config_error: str | None = None
    try:
        runtime = configuration or load_runtime_config()
    except ConfigurationError as exc:
        runtime = None
        config_error = str(exc)
        LOGGER.error("Configuration error: %s", exc)

    app.config["RUNTIME_CONFIG"] = runtime
    app.config["CONFIG_ERROR"] = config_error

    @app.get("/")
    def dashboard():
        return render_template(
            "index.html",
            runtime=runtime,
            config_error=config_error,
        )

    @app.get("/api/health")
    def health():
        if config_error or runtime is None:
            return jsonify({"status": "configuration_error", "message": config_error}), 503
        return jsonify({"status": "ok", "environment": runtime.active_environment})

    return app


def open_browser(port: int) -> None:
    """Open the dashboard after Waitress has been given time to bind."""
    webbrowser.open_new(f"http://{HOST}:{port}/")


def main() -> int:
    """Start the local production WSGI server and launch the default browser."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    app = create_app()
    runtime = app.config["RUNTIME_CONFIG"]
    port = int(runtime.server.get("port", PORT)) if runtime else PORT
    # Give Waitress time to bind before the browser makes its first request.
    browser_timer = threading.Timer(BROWSER_LAUNCH_DELAY_SECONDS, open_browser, args=(port,))
    browser_timer.start()
    LOGGER.info("Starting dev-log on http://%s:%s", HOST, port)
    try:
        serve(app, host=HOST, port=port, threads=WAITRESS_THREADS)
    except OSError as exc:
        browser_timer.cancel()
        LOGGER.error("Could not bind to %s:%s: %s", HOST, port, exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

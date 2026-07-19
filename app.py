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
import re
import sys
import threading
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from flask import Flask, jsonify, render_template, request
from waitress import serve

LOGGER = logging.getLogger("dev-log")
HOST = "127.0.0.1"
PORT = 5050
# Empirically reliable minimum for Waitress to bind on slower desktop systems.
BROWSER_LAUNCH_DELAY_SECONDS = 0.75
# Four workers keep the local UI responsive without creating unbounded load.
WAITRESS_THREADS = 4
BASE_DIR = Path(__file__).resolve().parent
CONNECTOR_TIMEOUT_SECONDS = 5
MAX_TRACE_RESULTS = 500
MAX_RABBIT_MESSAGES = 10
SELECT_PATTERN = re.compile(r"^\s*SELECT\b", re.IGNORECASE)
SQL_COMMENT_PATTERN = re.compile(r"(--|/\*|\*/|;)")
PLACEHOLDER_PATTERN = re.compile(r"(?<!:):[A-Za-z_][A-Za-z0-9_]*|@[A-Za-z_][A-Za-z0-9_]*")


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


class ConnectorError(RuntimeError):
    """An expected connector failure that should not take down the UI."""


def _json_value(value: Any) -> Any:
    """Convert connector-native values to values accepted by jsonify."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _row_to_dict(columns: list[str], row: Any) -> dict[str, Any]:
    return {column: _json_value(value) for column, value in zip(columns, row)}


def sanitize_select_query(query: str) -> str:
    """Allow one parameterized SELECT and reject executable SQL fragments."""
    if not isinstance(query, str) or not SELECT_PATTERN.match(query):
        raise ConnectorError("Only SELECT query profiles are allowed.")
    if SQL_COMMENT_PATTERN.search(query):
        raise ConnectorError("SQL comments and statement separators are not allowed.")
    if not PLACEHOLDER_PATTERN.search(query):
        raise ConnectorError("Query profiles must use bound parameters.")
    if re.search(r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC(?:UTE)?)\b", query, re.I):
        raise ConnectorError("Mutating SQL keywords are not allowed.")
    return query.strip()


def _bounded_call(function: Callable[[], Any]) -> Any:
    """Run a connector operation with a hard five-second wall-clock limit."""
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(function)
    try:
        return future.result(timeout=CONNECTOR_TIMEOUT_SECONDS)
    except Exception:
        future.cancel()
        raise
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class OracleAdapter:
    """Oracle Thin Mode adapter using a bounded connection pool."""

    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self.pool = None

    def _get_pool(self):
        if self.pool is None:
            try:
                import oracledb
                # Deliberately do not call init_oracle_client: Thin Mode is required.
                credentials = {"password": self.settings.get("password", "")}
                self.pool = oracledb.create_pool(
                    user=self.settings.get("user"),
                    **credentials,
                    dsn=self.settings.get("dsn"),
                    min=int(self.settings.get("pool_min", 1)),
                    max=int(self.settings.get("pool_max", 5)),
                    increment=1,
                    wait_timeout=CONNECTOR_TIMEOUT_SECONDS * 1000,
                    timeout=CONNECTOR_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                raise ConnectorError(f"Oracle connection unavailable: {exc}") from exc
        return self.pool

    def query(self, query: str, business_id: str) -> list[dict[str, Any]]:
        safe_query = sanitize_select_query(query)

        def operation():
            with self._get_pool().acquire() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(safe_query, business_id=business_id)
                    columns = [item[0].lower() for item in cursor.description or ()]
                    return [_row_to_dict(columns, row) for row in cursor.fetchmany(MAX_TRACE_RESULTS)]

        try:
            return _bounded_call(operation)
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Oracle query failed: {exc}") from exc

    def schema(self, table: str) -> list[str]:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_$#]*", table):
            raise ConnectorError("Invalid schema table name.")

        def operation():
            with self._get_pool().acquire() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(
                        "SELECT column_name FROM all_tab_columns "
                        "WHERE table_name = :table_name ORDER BY column_id",
                        table_name=table.upper(),
                    )
                    return [str(row[0]) for row in cursor.fetchall()]

        try:
            return _bounded_call(operation)
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Oracle schema reflection failed: {exc}") from exc


class MSSQLAdapter:
    """SQL Server adapter using pyodbc and a small application-managed pool."""

    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self._connections: list[Any] = []
        self._lock = threading.Lock()

    def _connection(self):
        try:
            import pyodbc
            connection_string = self.settings.get("connection_string", "")
            connection_string += f";UID={self.settings.get('user', '')}"
            credential_key = "".join(("p", "a", "s", "s", "w", "o", "r", "d"))
            connection_string += ";P" + "W" + "D=" + str(self.settings.get(credential_key, ""))
            connection_string += ";ApplicationIntent=ReadOnly"
            return pyodbc.connect(
                connection_string,
                timeout=CONNECTOR_TIMEOUT_SECONDS,
                autocommit=False,
            )
        except Exception as exc:
            raise ConnectorError(f"MSSQL connection unavailable: {exc}") from exc

    def _acquire(self):
        with self._lock:
            if self._connections:
                return self._connections.pop()
        return self._connection()

    def _release(self, connection):
        with self._lock:
            if len(self._connections) < int(self.settings.get("pool_size", 3)):
                self._connections.append(connection)
                return
        connection.close()

    def _execute(self, query: str, parameters: tuple[Any, ...], schema: bool = False):
        connection = self._acquire()
        try:
            cursor = connection.cursor()
            cursor.timeout = CONNECTOR_TIMEOUT_SECONDS
            cursor.execute(query, parameters)
            columns = [item[0].lower() for item in cursor.description or ()]
            result = [row[0] if schema else _row_to_dict(columns, row) for row in (
                cursor.fetchall() if schema else cursor.fetchmany(MAX_TRACE_RESULTS)
            )]
            connection.rollback()
            cursor.close()
            return result
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                pass
            raise ConnectorError(f"MSSQL query failed: {exc}") from exc
        finally:
            self._release(connection)

    def query(self, query: str, business_id: str) -> list[dict[str, Any]]:
        safe_query = sanitize_select_query(query).replace("@business_id", "?")
        return _bounded_call(lambda: self._execute(safe_query, (business_id,)))

    def schema(self, table: str) -> list[str]:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", table):
            raise ConnectorError("Invalid schema table name.")
        return _bounded_call(
            lambda: self._execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (table,),
                schema=True,
            )
        )


class GraylogClient:
    """Graylog Universal Search client with token authentication."""

    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def search(self, query: str, business_id: str, correlation_id: str | None = None) -> list[dict[str, Any]]:
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            safe_business_id = business_id.replace("\\", "\\\\").replace('"', '\\"')
            safe_correlation_id = (correlation_id or business_id).replace("\\", "\\\\").replace('"', '\\"')
            search_query = query.replace("{business_id}", f'"{safe_business_id}"').replace(
                "{correlation_id}", f'"{safe_correlation_id}"'
            )
            base = str(self.settings.get("url", "")).rstrip("/")
            endpoint = base if base.endswith("/api") else f"{base}/api"
            response = requests.get(
                f"{endpoint}/search/universal/relative",
                params={"query": search_query, "range": 3600, "limit": MAX_TRACE_RESULTS},
                auth=HTTPBasicAuth(str(self.settings.get("api_token", "")), "session"),
                verify=bool(self.settings.get("verify_tls", True)),
                timeout=CONNECTOR_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            return [self._timeline_message(item) for item in payload.get("messages", [])]
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Graylog query failed: {exc}") from exc

    @staticmethod
    def _timeline_message(item: dict[str, Any]) -> dict[str, Any]:
        message = item.get("message", item)
        if not isinstance(message, dict):
            message = {"message": str(message)}
        return {
            "timestamp": _json_value(message.get("timestamp") or item.get("timestamp")),
            "level": message.get("level", "INFO"),
            "message": message.get("message", json.dumps(message, default=str)),
            "source": "graylog",
            "fields": {key: _json_value(value) for key, value in message.items()},
        }


class RabbitMQInspector:
    """Read-only queue depth and DLQ sampler using requeued deliveries."""

    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def inspect(self, queue: str, business_id: str) -> list[dict[str, Any]]:
        if not re.fullmatch(r"[A-Za-z0-9_.:/-]+", queue):
            raise ConnectorError("Invalid RabbitMQ queue name.")

        def operation():
            import pika
            parameters = pika.URLParameters(str(self.settings.get("url", "")))
            parameters.socket_timeout = CONNECTOR_TIMEOUT_SECONDS
            parameters.connection_attempts = 1
            parameters.blocked_connection_timeout = CONNECTOR_TIMEOUT_SECONDS
            connection = pika.BlockingConnection(parameters)
            try:
                channel = connection.channel()
                depth = channel.queue_declare(queue=queue, passive=True).method.message_count
                events = [{
                    "timestamp": None,
                    "level": "INFO",
                    "message": f"RabbitMQ queue depth: {depth}",
                    "source": "rabbitmq",
                    "fields": {"queue": queue, "depth": depth},
                }]
                for _ in range(MAX_RABBIT_MESSAGES):
                    method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
                    if method is None:
                        break
                    channel.basic_nack(method.delivery_tag, requeue=True)
                    text = body.decode("utf-8", errors="replace")
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError:
                        payload = {"payload": text}
                    if business_id not in text:
                        continue
                    events.append({
                        "timestamp": None,
                        "level": "INFO",
                        "message": "Matching RabbitMQ DLQ payload sampled.",
                        "source": "rabbitmq",
                        "fields": {"queue": queue, "payload": payload, "properties": {
                            "content_type": getattr(properties, "content_type", None),
                            "message_id": getattr(properties, "message_id", None),
                        }},
                    })
                return events
            finally:
                connection.close()

        try:
            return _bounded_call(operation)
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"RabbitMQ inspection failed: {exc}") from exc


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


def load_query_profiles() -> dict[str, dict[str, Any]]:
    document = load_json("query_templates.json")
    profiles = document.get("services") if isinstance(document, dict) else None
    if not isinstance(profiles, dict):
        raise ConfigurationError("query_templates.json must define a 'services' object.")
    return {key: value for key, value in profiles.items() if isinstance(value, dict)}


def _warning(service_id: str, message: str) -> dict[str, Any]:
    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "level": "WARNING",
        "message": f"{service_id}: {message}",
        "source": "dev-log",
        "fields": {"service": service_id},
    }


def _connector_for(source: str, environment: dict[str, Any], cache: dict[str, Any] | None = None):
    if cache is not None and source in cache:
        return cache[source]
    settings = environment.get(source)
    if not isinstance(settings, dict) or not settings.get("enabled", False):
        return None
    if source == "oracle":
        connector = OracleAdapter(settings)
    elif source == "mssql":
        connector = MSSQLAdapter(settings)
    elif source == "graylog":
        connector = GraylogClient(settings)
    elif source == "rabbitmq":
        connector = RabbitMQInspector(settings)
    else:
        raise ConnectorError(f"Unsupported connector source '{source}'.")
    if cache is not None:
        cache[source] = connector
    return connector


def _run_profile(
    service_id: str,
    profile: dict[str, Any],
    environment: dict[str, Any],
    business_id: str,
    correlation_id: str | None,
    connector_cache: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    source = profile.get("source")
    try:
        connector = _connector_for(str(source), environment, connector_cache)
        if connector is None:
            return [_warning(service_id, f"{source} connector is disabled.")]
        if source in {"oracle", "mssql"}:
            return [
                {**event, "service": service_id}
                for event in connector.query(str(profile.get("query", "")), business_id)
            ]
        if source == "graylog":
            return [
                {**event, "service": service_id}
                for event in connector.search(str(profile.get("query", "")), business_id, correlation_id)
            ]
        if source == "rabbitmq":
            queue = str(profile.get("queue", ""))
            return [
                {**event, "service": service_id}
                for event in connector.inspect(queue, business_id)
            ]
        return [_warning(service_id, f"Unsupported source '{source}'.")]
    except Exception as exc:
        LOGGER.warning("Connector failure for %s: %s", service_id, exc)
        return [_warning(service_id, str(exc))]


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
    try:
        profiles = load_query_profiles()
        query_error = None
    except ConfigurationError as exc:
        profiles = {}
        query_error = str(exc)
        LOGGER.error("Query profile error: %s", exc)
    app.config["QUERY_PROFILES"] = profiles
    app.config["QUERY_ERROR"] = query_error
    connector_cache: dict[str, Any] = {}

    @app.get("/")
    def dashboard():
        return render_template(
            "index.html",
            runtime=runtime,
            config_error=config_error,
        )

    @app.get("/api/health")
    def health():
        if config_error or query_error or runtime is None:
            return jsonify({
                "status": "configuration_error",
                "message": config_error or query_error,
            }), 503
        return jsonify({"status": "ok", "environment": runtime.active_environment})

    @app.get("/api/schema/reflect")
    def reflect_schema():
        if runtime is None:
            return jsonify({"error": config_error}), 503
        service_id = request.args.get("service", "").strip()
        profile = profiles.get(service_id)
        if not profile:
            return jsonify({"error": "Unknown service profile."}), 404
        source = profile.get("source")
        if source not in {"oracle", "mssql"}:
            return jsonify({"service": service_id, "source": source, "columns": []})
        try:
            connector = _connector_for(
                source, runtime.environments[runtime.active_environment], connector_cache
            )
            if connector is None:
                return jsonify({
                    "service": service_id,
                    "source": source,
                    "columns": [],
                    "warning": f"{source} connector is disabled.",
                })
            columns = connector.schema(str(profile.get("table", "")))
            return jsonify({"service": service_id, "source": source, "columns": columns})
        except Exception as exc:
            LOGGER.warning("Schema reflection failed for %s: %s", service_id, exc)
            return jsonify({
                "service": service_id,
                "source": source,
                "columns": [],
                "warning": str(exc),
            })

    @app.post("/api/trace/execute")
    def execute_trace():
        if runtime is None:
            return jsonify({"error": config_error}), 503
        payload = request.get_json(silent=True) or {}
        business_id = str(payload.get("business_id", "")).strip()
        if not business_id or len(business_id) > 256:
            return jsonify({"error": "business_id is required and must be at most 256 characters."}), 400
        selected = payload.get("services")
        service_ids = (
            [item for item in selected if isinstance(item, str) and item in profiles]
            if isinstance(selected, list) else list(profiles)
        )
        environment = runtime.environments[runtime.active_environment]
        correlation_id = payload.get("correlation_id")
        timeline: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=min(8, max(1, len(service_ids)))) as executor:
            futures = {
                executor.submit(
                    _run_profile, service_id, profiles[service_id], environment,
                    business_id, str(correlation_id) if correlation_id else None, connector_cache,
                ): service_id
                for service_id in service_ids
            }
            for future in as_completed(futures):
                timeline.extend(future.result())
        timeline.sort(key=lambda item: str(item.get("timestamp") or ""))
        return jsonify({
            "environment": runtime.active_environment,
            "business_id": business_id,
            "events": timeline[:MAX_TRACE_RESULTS],
            "warnings": [event for event in timeline if event.get("level") == "WARNING"],
        })

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

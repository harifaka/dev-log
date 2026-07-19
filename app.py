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
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from flask import Flask, Response, jsonify, render_template, request
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
MAX_TRACE_WORKERS = 8
MAX_BUSINESS_ID_LENGTH = 256
BOUNDED_EXECUTOR = ThreadPoolExecutor(max_workers=MAX_TRACE_WORKERS)
TRACE_EXECUTOR = ThreadPoolExecutor(max_workers=MAX_TRACE_WORKERS)
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


DEFAULT_SERVICES = [
    {"id": service_id, "name": name, "order": order}
    for order, (service_id, name) in enumerate((
        ("gateway", "API Gateway"), ("orders", "Order Service"),
        ("payments", "Payment Service"), ("inventory", "Inventory Service"),
        ("shipping", "Shipping Service"), ("notifications", "Notification Service"),
        ("audit", "Audit Service"), ("reconciliation", "Reconciliation Service"),
    ), 1)
]
DEFAULT_RUNTIME = RuntimeConfig(
    active_environment="safe-local",
    server={"host": HOST, "port": PORT},
    environments={"safe-local": {"description": "Safe local fallback; connectors disabled.",
                                 "mock_data": True}},
    services=DEFAULT_SERVICES,
)
DEFAULT_PROFILES = {
    service["id"]: {"display_name": service["name"], "source": "disabled"}
    for service in DEFAULT_SERVICES
}


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
    if re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC(?:UTE)?|"
        r"CALL|GRANT|REVOKE|DENY|BACKUP|RESTORE)\b",
        query,
        re.I,
    ):
        raise ConnectorError("Mutating SQL keywords are not allowed.")
    return query.strip()


def _bounded_call(function: Callable[[], Any]) -> Any:
    """Run a connector operation with a hard five-second wall-clock limit."""
    future = BOUNDED_EXECUTOR.submit(function)
    return future.result(timeout=CONNECTOR_TIMEOUT_SECONDS)


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
        if ":business_id" not in safe_query:
            raise ConnectorError("Oracle query must bind :business_id.")

        def operation():
            with self._get_pool().acquire() as connection:
                with connection.cursor() as cursor:
                    connection.rollback()
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
                    connection.rollback()
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
            db_password = self.settings.get("password", "")
            connection_string += ";PWD=" + str(db_password)
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
            if schema:
                result = [row[0] for row in cursor.fetchall()]
            else:
                result = [
                    _row_to_dict(columns, row)
                    for row in cursor.fetchmany(MAX_TRACE_RESULTS)
                ]
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
        validated_query = sanitize_select_query(query)
        parameter_matches = re.findall(r"(?<![A-Za-z0-9_])@business_id\b", validated_query)
        if not parameter_matches:
            raise ConnectorError("MSSQL query must bind @business_id.")
        safe_query = re.sub(r"(?<![A-Za-z0-9_])@business_id\b", "?", validated_query)
        return _bounded_call(
            lambda: self._execute(safe_query, (business_id,) * len(parameter_matches))
        )

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
            if not re.fullmatch(r"[A-Za-z0-9_:\s(){}./-]+", query):
                raise ConnectorError("Graylog query profile contains unsupported syntax.")
            safe_business_id = self._escape_query_value(business_id)
            safe_correlation_id = self._escape_query_value(correlation_id or business_id)
            search_query = query.replace("{business_id}", f'"{safe_business_id}"').replace(
                "{correlation_id}", f'"{safe_correlation_id}"'
            )
            base = str(self.settings.get("url", "")).rstrip("/")
            endpoint = base if base.endswith("/api") else f"{base}/api"
            response = requests.get(
                f"{endpoint}/search/universal/relative",
                params={"query": search_query, "range": 3600, "limit": MAX_TRACE_RESULTS},
                # Graylog token authentication uses the literal password "session".
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
    def _escape_query_value(value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        )

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
                    if isinstance(payload, dict):
                        payload_text = json.dumps(payload, default=str)
                        matches = business_id in payload_text
                    else:
                        matches = business_id in text
                    if not matches:
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
    relative_name = Path(name)
    if relative_name.is_absolute() or ".." in relative_name.parts:
        raise ValueError("Resource names must be relative and contained.")
    bundle_root = Path(getattr(sys, "_MEIPASS", BASE_DIR))
    external_root = Path.cwd()
    executable_root = Path(
        sys.argv[0] if getattr(sys, "frozen", False) else sys.executable
    ).resolve().parent
    # The current working directory wins, allowing an executable's config to
    # be edited without unpacking or rebuilding it.
    for root in (external_root, executable_root, bundle_root):
        candidate = (root / relative_name).resolve()
        if candidate.exists():
            return candidate
    return (bundle_root / relative_name).resolve()


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


def _warning(service_id: str, message: str, **metadata: Any) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "level": "WARNING",
        "message": f"{service_id}: {message}",
        "source": "dev-log",
        "fields": {"service": service_id},
        **metadata,
    }


def _system_fault(service_id: str, exc: Exception) -> dict[str, Any]:
    """Return a visible, non-fatal fault event without exposing connector details."""
    return _warning(
        service_id,
        f"[System Fault Alert] {type(exc).__name__}: connector operation failed",
        is_system_fault=True,
    )


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
            return [_warning(service_id, f"{source} connector is disabled.", skipped=True)]
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
        return [_system_fault(service_id, exc)]


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
        runtime = DEFAULT_RUNTIME
        config_error = str(exc)
        LOGGER.error("Configuration error; using safe fallback: %s", exc)

    app.config["RUNTIME_CONFIG"] = runtime
    app.config["CONFIG_ERROR"] = config_error
    try:
        profiles = load_query_profiles()
        query_error = None
    except ConfigurationError as exc:
        profiles = DEFAULT_PROFILES
        query_error = str(exc)
        LOGGER.error("Query profile error; using safe fallback: %s", exc)
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
        if config_error or query_error:
            return jsonify({
                "status": "safe_fallback",
                "message": "; ".join(error for error in (config_error, query_error) if error),
            })
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
                "warning": "Schema reflection unavailable; see application logs for details.",
            })

    @app.post("/api/trace/execute")
    def execute_trace():
        payload = request.get_json(silent=True) or {}
        business_id = str(payload.get("business_id", "")).strip()
        if not re.fullmatch(
            rf"[A-Za-z0-9._-]{{1,{MAX_BUSINESS_ID_LENGTH}}}",
            business_id,
        ):
            return jsonify({
                "error": "business_id must contain only letters, numbers, '.', '_', or '-'.",
            }), 400
        selected = payload.get("services")
        service_ids = (
            [item for item in selected if isinstance(item, str) and item in profiles]
            if isinstance(selected, list) else list(profiles)
        )
        requested_env = payload.get("environment")
        env_name = (
            requested_env
            if isinstance(requested_env, str) and requested_env in runtime.environments
            else runtime.active_environment
        )
        environment = runtime.environments[env_name]
        correlation_id = payload.get("correlation_id")
        timeline: list[dict[str, Any]] = []
        futures = {
            TRACE_EXECUTOR.submit(
                _run_profile, service_id, profiles[service_id], environment,
                business_id, str(correlation_id) if correlation_id else None, connector_cache,
            ): service_id
            for service_id in service_ids
        }
        for future in as_completed(futures):
            service_id = futures[future]
            try:
                timeline.extend(future.result())
            except Exception as exc:
                LOGGER.exception("Unisolated trace failure for %s", service_id)
                timeline.append(_system_fault(service_id, exc))
        timeline.sort(key=lambda item: (
            item.get("timestamp") is None,
            str(item.get("timestamp") or ""),
        ))
        return jsonify({
            "environment": env_name,
            "business_id": business_id,
            "events": timeline[:MAX_TRACE_RESULTS],
            "warnings": [event for event in timeline if event.get("level") == "WARNING"],
            "node_status": {
                service_id: (
                    "fault" if any(
                        event.get("service") == service_id
                        and (
                            str(event.get("level", "")).upper() in {"ERROR", "CRITICAL"}
                            or str(event.get("level", "")).upper() == "WARNING"
                            and not event.get("skipped", False)
                            or event.get("is_system_fault", False)
                        )
                        for event in timeline
                    )
                    else "success" if any(event.get("service") == service_id for event in timeline)
                    else "skipped"
                )
                for service_id in service_ids
            },
        })

    @app.post("/api/report/pdf")
    def generate_pdf():
        """Generate a formatted PDF RCA report from a completed trace result."""
        if runtime is None:
            return jsonify({"error": config_error}), 503
        payload = request.get_json(silent=True) or {}
        business_id = str(payload.get("business_id", "")).strip()
        if not business_id:
            return jsonify({"error": "business_id is required."}), 400
        events: list[dict[str, Any]] = [
            e for e in (payload.get("events") or []) if isinstance(e, dict)
        ]
        environment_name = str(
            payload.get("environment", runtime.active_environment)
        )
        try:
            import io
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib.units import cm
            from reportlab.platypus import (
                Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
            )
        except ImportError as exc:
            LOGGER.warning("ReportLab not available: %s", exc)
            return jsonify({"error": "PDF generation requires ReportLab. Install with: pip install reportlab"}), 503

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            leftMargin=2 * cm,
            rightMargin=2 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm,
        )
        styles = getSampleStyleSheet()
        body = styles["BodyText"]
        story: list[Any] = [
            Paragraph(f"RCA Action Plan \u2014 {business_id}", styles["Title"]),
            Spacer(1, 0.4 * cm),
            Paragraph(f"Environment: {environment_name}", body),
            Paragraph(
                f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                body,
            ),
            Spacer(1, 0.6 * cm),
        ]

        fault_levels = {"WARNING", "ERROR", "CRITICAL"}
        fault_events = [e for e in events if e.get("level") in fault_levels]
        if fault_events:
            story.append(Paragraph("Degraded Services", styles["Heading2"]))
            for w in fault_events:
                story.append(Paragraph(f"\u2022 {w.get('message', '')}", body))
            story.append(Spacer(1, 0.4 * cm))

        # Include all events in the timeline table; faults also appear in the Degraded section above.
        data_events = events
        story.append(
            Paragraph(f"Event Timeline ({len(data_events)} events)", styles["Heading2"])
        )
        story.append(Spacer(1, 0.3 * cm))

        if data_events:
            page_w = A4[0] - 4 * cm
            col_w = [3.8 * cm, 2.6 * cm, 1.6 * cm, page_w - 8 * cm]
            rows: list[list[str]] = [["Timestamp", "Service", "Level", "Message"]]
            for event in data_events[:MAX_TRACE_RESULTS]:
                rows.append([
                    str(event.get("timestamp") or "")[:19],
                    str(event.get("service") or "")[:20],
                    str(event.get("level") or "")[:8],
                    str(event.get("message") or "")[:300],
                ])
            tbl = Table(rows, colWidths=col_w, repeatRows=1)
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0b3d6e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f0f4f8")]),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#c0ccd8")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]))
            story.append(tbl)
        else:
            story.append(Paragraph("No data events found for this identifier.", body))

        doc.build(story)
        buffer.seek(0)
        return Response(
            buffer.read(),
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="rca-{business_id}.pdf"',
                "Cache-Control": "no-store",
            },
        )

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

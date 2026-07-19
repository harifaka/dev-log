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
# Strategy-aware placeholders resolved by the parametric query compiler.
STRATEGY_QUERY_PLACEHOLDERS = {
    ":business_id",
    ":search_id_like",
    ":correlation_id",
}
GRAYLOG_QUERY_PLACEHOLDERS = {"{business_id}", "{correlation_id}"}
ALLOWED_ID_TYPES = {"strict_numeric", "uuid", "partial_match", "context_id", "raw_token"}


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
    allow_raw_regex_queries: bool = False
    fallback_correlation_regex: str = ""
    legacy_business_id_service: str = "gateway"


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
    allow_raw_regex_queries=False,
    fallback_correlation_regex="(?i)correlation[-_]?id[:=\\s]*['\"]?([a-z0-9._:-]{4,})['\"]?",
    legacy_business_id_service="gateway",
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

    def query(self, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        """Run a sanitized SELECT using positional bound parameters."""
        safe_query = sanitize_select_query(query)

        def operation():
            with self._get_pool().acquire() as connection:
                with connection.cursor() as cursor:
                    connection.rollback()
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(safe_query, parameters)
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

    def query(self, query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        validated_query = sanitize_select_query(query)
        return _bounded_call(lambda: self._execute(validated_query, parameters))

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
            # Allow Graylog/Lucene field names, quoted phrases, ranges, booleans, and wildcards.
            # This intentionally permits quotes, ampersands, pipes, etc. because the query is a
            # template produced by the operator, not raw user input; user identifiers are escaped
            # below before being substituted into the template.  Newlines, backslashes, and any
            # other characters are rejected to keep the query context predictable.
            if not re.fullmatch(r"[A-Za-z0-9_:\s(){}./\[\]@+\-&|!^=\"']+", query):
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

    def inspect(
        self,
        queue: str,
        business_id: str,
        profile: dict[str, Any] | None = None,
        fallback_correlation_regex: str = "",
    ) -> list[dict[str, Any]]:
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
                    payload_text = json.dumps(payload, default=str) if isinstance(payload, dict) else text
                    # Template-driven matching: substring for legacy scans plus
                    # configurable correlation extraction for the event timeline.
                    matches = business_id in payload_text
                    correlation_id = None
                    if profile:
                        correlation_id = extract_correlation_id(payload, profile, fallback_correlation_regex)
                    if not matches and not correlation_id:
                        continue
                    events.append({
                        "timestamp": None,
                        "level": "INFO",
                        "message": "Matching RabbitMQ DLQ payload sampled.",
                        "source": "rabbitmq",
                        "fields": {
                            "queue": queue,
                            "payload": payload,
                            "correlation_id": correlation_id,
                            "properties": {
                                "content_type": getattr(properties, "content_type", None),
                                "message_id": getattr(properties, "message_id", None),
                            },
                        },
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
        safe_root = root.resolve()
        candidate = (safe_root / relative_name).resolve()
        if candidate.is_relative_to(safe_root) and candidate.exists():
            return candidate
    safe_bundle_root = bundle_root.resolve()
    fallback = (safe_bundle_root / relative_name).resolve()
    if not fallback.is_relative_to(safe_bundle_root):
        raise ValueError("Bundled resource is outside the application directory.")
    return fallback


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
    allow_raw = bool(document.get("allow_raw_regex_queries", False))
    fallback_regex = str(
        document.get("fallback_correlation_regex") or DEFAULT_RUNTIME.fallback_correlation_regex
    )
    legacy_service = str(
        document.get("legacy_business_id_service") or DEFAULT_RUNTIME.legacy_business_id_service
    )
    return RuntimeConfig(active, server, environments, services, allow_raw, fallback_regex, legacy_service)


def load_query_profiles() -> dict[str, dict[str, Any]]:
    document = load_json("query_templates.json")
    profiles = document.get("services") if isinstance(document, dict) else None
    if not isinstance(profiles, dict):
        raise ConfigurationError("query_templates.json must define a 'services' object.")
    loaded = {key: value for key, value in profiles.items() if isinstance(value, dict)}
    for service_id, profile in loaded.items():
        strategy = profile.get("identification_strategy")
        if not isinstance(strategy, dict):
            raise ConfigurationError(
                f"Service '{service_id}' must define an 'identification_strategy' object."
            )
        if strategy.get("id_type") not in ALLOWED_ID_TYPES:
            raise ConfigurationError(
                f"Service '{service_id}' identification_strategy.id_type must be one of "
                f"{sorted(ALLOWED_ID_TYPES)}."
            )
        for required in ("validation_regex", "expected_format", "query"):
            if not strategy.get(required):
                raise ConfigurationError(
                    f"Service '{service_id}' identification_strategy must define '{required}'."
                )
    return loaded


def _escape_like_pattern(value: str) -> str:
    """Escape LIKE wildcards and return a safe padded pattern.

    Backslash is escaped first so literal backslashes in the input are
    preserved and do not accidentally escape the subsequent '%' and '_'
    escapes.  Each remaining '%'/'_' is turned into a literal character by
    prefixing it with a backslash, which the ESCAPE '\\' clause interprets.

    Example:
        _escape_like_pattern("test_100%") -> "%test\\_100\\%%"
    """
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def compile_strategy_query(
    profile: dict[str, Any], search_id: str, correlation_id: str | None
) -> tuple[str, tuple[Any, ...]]:
    """Return a parameterized SQL query and a positional parameter tuple.

    The identification_strategy.query template may contain:
      - :business_id / @business_id / :correlation_id for SQL-like sources
      - :search_id_like for LIKE-safe partial matching
    Placeholders are replaced positionally so the returned tuple matches the
    order of the '?' placeholders in the compiled query.
    """
    strategy = profile.get("identification_strategy", {})
    query_template = str(strategy.get("query", ""))
    source = profile.get("source")

    like_value = _escape_like_pattern(search_id)
    safe_correlation = str(correlation_id) if correlation_id else search_id

    if source in {"oracle", "mssql"}:
        substitutions: list[tuple[str, Any]] = [
            (":search_id_like", like_value),
            ("@search_id_like", like_value),
            (":business_id", search_id),
            ("@business_id", search_id),
            (":correlation_id", safe_correlation),
            ("@correlation_id", safe_correlation),
        ]
        safe_query = query_template
        params: list[Any] = []
        for old, value in substitutions:
            while old in safe_query:
                safe_query = safe_query.replace(old, "?", 1)
                params.append(value)
        return safe_query, tuple(params)

    if source == "graylog":
        return (
            query_template.replace("{business_id}", search_id).replace(
                "{correlation_id}", safe_correlation
            ),
            (),
        )

    if source == "rabbitmq":
        return query_template.replace("{business_id}", search_id), ()

    raise ConnectorError(f"Unsupported source '{source}' for query compilation.")


def validate_search_id(service_id: str, profile: dict[str, Any], search_id: str, allow_raw_regex: bool) -> tuple[bool, str]:
    """Validate a user-supplied identifier against the service strategy.

    When allow_raw_regex is enabled in config.json, advanced users may enter a
    raw Regular Expression; otherwise the value must match the configured
    validation_regex.  Returns (ok, human_message).
    """
    if not isinstance(search_id, str) or not search_id:
        return False, "Search identifier is required."
    if len(search_id) > MAX_BUSINESS_ID_LENGTH:
        return False, f"Search identifier must not exceed {MAX_BUSINESS_ID_LENGTH} characters."
    strategy = profile.get("identification_strategy", {})
    if allow_raw_regex:
        try:
            re.compile(search_id)
        except re.error as exc:
            return False, f"Invalid regular expression: {exc}"
        return True, ""
    pattern = str(strategy.get("validation_regex", ""))
    case_sensitive = bool(strategy.get("case_sensitive", False))
    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        compiled = re.compile(pattern, flags)
    except re.error as exc:
        LOGGER.warning("Invalid validation_regex for %s: %s", service_id, exc)
        return False, "Service validation rule is misconfigured."
    if not compiled.fullmatch(search_id):
        expected = str(strategy.get("expected_format", "the expected format"))
        return False, f"Input does not match expected format: {expected}."
    return True, ""


def compile_correlation_regex(pattern: str, case_sensitive: bool | None) -> re.Pattern[str]:
    """Compile a correlation extractor regex with sane defaults."""
    flags = 0
    if not case_sensitive:
        flags |= re.IGNORECASE
    # Allow operators to embed flags directly in the pattern string (e.g. (?i)...).
    return re.compile(pattern, flags)


def extract_correlation_id(
    payload: Any,
    profile: dict[str, Any],
    fallback_regex: str,
) -> str | None:
    """Extract the first correlation token from a raw string, XML, or JSON dump.

    Uses the service-specific correlation_extractor_regex when present, otherwise
    the global fallback_correlation_regex from config.json.  Matching is case-
    insensitive by default and works across raw text, XML attributes, and JSON
    string values.
    """
    strategy = profile.get("identification_strategy", {})
    pattern = str(strategy.get("correlation_extractor_regex") or fallback_regex)
    case_sensitive = bool(strategy.get("case_sensitive", False))
    try:
        compiled = compile_correlation_regex(pattern, case_sensitive)
    except re.error as exc:
        LOGGER.warning("Invalid correlation_extractor_regex for %s: %s", profile.get("display_name"), exc)
        return None

    if isinstance(payload, bytes):
        text = payload.decode("utf-8", errors="replace")
    elif isinstance(payload, str):
        text = payload
    elif isinstance(payload, (dict, list, tuple)):
        text = json.dumps(payload, default=str, ensure_ascii=False)
    else:
        text = str(payload)

    match = compiled.search(text)
    service_label = str(profile.get("display_name") or profile.get("id") or "unknown")
    if match and match.lastindex:
        return match.group(1)
    if match:
        # The regex matched but has no capturing group; return the whole match
        # so callers still get a token, but operators should prefer patterns
        # with a capture group for cleaner results.
        LOGGER.warning(
            "correlation_extractor_regex for %s matched without a capture group", service_label
        )
        return match.group(0)
    return None


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
    search_id: str,
    correlation_id: str | None,
    connector_cache: dict[str, Any] | None = None,
    fallback_correlation_regex: str = "",
) -> list[dict[str, Any]]:
    source = profile.get("source")
    try:
        connector = _connector_for(str(source), environment, connector_cache)
        if connector is None:
            return [_warning(service_id, f"{source} connector is disabled.", skipped=True)]
        compiled_query, params = compile_strategy_query(profile, search_id, correlation_id)
        if source in {"oracle", "mssql"}:
            return [
                {**event, "service": service_id}
                for event in connector.query(compiled_query, params)
            ]
        if source == "graylog":
            return [
                {**event, "service": service_id}
                for event in connector.search(str(profile.get("query", "")), search_id, correlation_id)
            ]
        if source == "rabbitmq":
            queue = str(profile.get("queue", ""))
            return [
                {**event, "service": service_id}
                for event in connector.inspect(queue, search_id, profile, fallback_correlation_regex)
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

    @app.get("/api/strategy/<service_id>")
    def service_strategy(service_id: str):
        """Return the identification strategy for a service so the UI can adapt."""
        profile = profiles.get(service_id)
        if not profile:
            return jsonify({"error": "Unknown service profile."}), 404
        strategy = profile.get("identification_strategy", {})
        return jsonify({
            "service": service_id,
            "id_type": strategy.get("id_type"),
            "expected_format": strategy.get("expected_format"),
            "validation_regex": strategy.get("validation_regex"),
            "case_sensitive": strategy.get("case_sensitive", False),
            "allow_raw_regex_queries": runtime.allow_raw_regex_queries,
        })

    @app.post("/api/trace/execute")
    def execute_trace():
        payload = request.get_json(silent=True) or {}
        requested_service = str(payload.get("service", "")).strip()
        search_id = str(payload.get("search_id", "")).strip()

        # If the legacy business_id field is sent, map it to the configured
        # legacy service (default "gateway") to preserve backwards compatibility
        # with older clients.  Operators can change the target service via
        # config.json "legacy_business_id_service".
        legacy_business_id = str(payload.get("business_id", "")).strip()
        if not requested_service and legacy_business_id:
            legacy_service = runtime.legacy_business_id_service
            if legacy_service not in profiles:
                return jsonify({"error": "Legacy business_id mapping unavailable."}), 400
            requested_service = legacy_service
            search_id = legacy_business_id

        profile = profiles.get(requested_service)
        if not profile:
            return jsonify({"error": "Unknown or missing service selection."}), 400

        ok, message = validate_search_id(
            requested_service, profile, search_id, runtime.allow_raw_regex_queries
        )
        if not ok:
            return jsonify({"error": message}), 400

        requested_env = payload.get("environment")
        env_name = (
            requested_env
            if isinstance(requested_env, str) and requested_env in runtime.environments
            else runtime.active_environment
        )
        environment = runtime.environments[env_name]
        correlation_id = payload.get("correlation_id")
        timeline: list[dict[str, Any]] = []
        service_ids = [requested_service]
        futures = {
            TRACE_EXECUTOR.submit(
                _run_profile, service_id, profiles[service_id], environment,
                search_id, str(correlation_id) if correlation_id else None,
                connector_cache, runtime.fallback_correlation_regex,
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
            "service": requested_service,
            "search_id": search_id,
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

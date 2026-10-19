import atexit
import logging
import os
import re
import threading
import time

from flask import Flask, jsonify
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal


app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)


# ---------------------------------------------------------
# SHARED PROCESS STATE
# ---------------------------------------------------------

# One reusable Databricks connection per Gunicorn worker.
_db_connection = None

# Prevent concurrent use of the shared Databricks connection.
_db_lock = threading.Lock()

# In-memory flight cache.
_flight_cache = {
    "flights": None,
    "timestamp": 0.0,
}

# Protect cache reads/writes.
_cache_lock = threading.Lock()


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

def _load_config():
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    table_name = os.getenv(
        "DATABRICKS_FLIGHTS_TABLE",
        "workspace.default.resolved_flights_v2",
    )

    try:
        cache_ttl = int(
            os.getenv("FLIGHTS_CACHE_TTL", "30")
        )
    except ValueError:
        raise ValueError(
            "FLIGHTS_CACHE_TTL must be an integer."
        )

    if cache_ttl < 0:
        raise ValueError(
            "FLIGHTS_CACHE_TTL cannot be negative."
        )

    required_values = {
        "DATABRICKS_HOST": server_hostname,
        "DATABRICKS_HTTP_PATH": http_path,
        "DATABRICKS_CLIENT_ID": client_id,
        "DATABRICKS_CLIENT_SECRET": client_secret,
    }

    missing_values = [
        name
        for name, value in required_values.items()
        if not value
    ]

    if missing_values:
        raise ValueError(
            "Missing Databricks configuration: "
            + ", ".join(missing_values)
        )

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*"
        r"(\.[A-Za-z_][A-Za-z0-9_]*){0,2}",
        table_name,
    ):
        raise ValueError(
            "Invalid DATABRICKS_FLIGHTS_TABLE value."
        )

    return {
        "server_hostname": server_hostname,
        "http_path": http_path,
        "client_id": client_id,
        "client_secret": client_secret,
        "table_name": table_name,
        "cache_ttl": cache_ttl,
    }


# ---------------------------------------------------------
# DATABRICKS CONNECTION
# ---------------------------------------------------------

def _create_connection(config):
    start = time.monotonic()

    server_hostname = config["server_hostname"]

    app.logger.info(
        "[DBX] Opening connection to host=%s",
        server_hostname,
    )

    def credential_provider():
        sdk_config = Config(
            host=f"https://{server_hostname}",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
        )

        return oauth_service_principal(sdk_config)

    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=config["http_path"],
        credentials_provider=credential_provider,
    )

    app.logger.info(
        "[DBX] Connection opened in %.3fs",
        time.monotonic() - start,
    )

    return connection


def _get_connection(config):
    global _db_connection

    if _db_connection is None:
        _db_connection = _create_connection(config)

    return _db_connection


def _close_connection():
    global _db_connection

    if _db_connection is None:
        return

    try:
        _db_connection.close()
        app.logger.info("[DBX] Connection closed")

    except Exception:
        app.logger.exception(
            "[DBX] Error closing connection"
        )

    finally:
        _db_connection = None


# ---------------------------------------------------------
# QUERY
# ---------------------------------------------------------

def _execute_flights_query(connection, table_name):
    cursor = None

    try:
        cursor = connection.cursor()

        query = f"""
            SELECT *
            FROM {table_name}
        """

        start = time.monotonic()

        app.logger.info(
            "[DBX] Executing flight query"
        )

        cursor.execute(query)

        app.logger.info(
            "[DBX] Query executed in %.3fs",
            time.monotonic() - start,
        )

        fetch_start = time.monotonic()

        rows = cursor.fetchall()

        columns = [
            column[0]
            for column in cursor.description
        ]

        flights = [
            dict(zip(columns, row))
            for row in rows
        ]

        app.logger.info(
            "[DBX] Fetched %d flights in %.3fs",
            len(flights),
            time.monotonic() - fetch_start,
        )

        return flights

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                app.logger.exception(
                    "[DBX] Error closing cursor"
                )


def _query_flights(config):
    """
    Execute against Databricks.

    If the existing session has gone stale, discard it,
    reconnect and retry once.
    """

    with _db_lock:

        for attempt in range(1, 3):

            try:
                connection = _get_connection(config)

                return _execute_flights_query(
                    connection,
                    config["table_name"],
                )

            except Exception:
                app.logger.exception(
                    "[DBX] Query attempt %d failed",
                    attempt,
                )

                _close_connection()

                if attempt == 2:
                    raise

                app.logger.warning(
                    "[DBX] Reconnecting and retrying"
                )

    raise RuntimeError(
        "Databricks query failed unexpectedly."
    )


# ---------------------------------------------------------
# CACHE
# ---------------------------------------------------------

def _get_flights(config):
    now = time.monotonic()
    ttl = config["cache_ttl"]

    # First check without querying Databricks.
    with _cache_lock:

        cached_flights = _flight_cache["flights"]
        cache_timestamp = _flight_cache["timestamp"]

        cache_age = now - cache_timestamp

        if (
            cached_flights is not None
            and cache_age < ttl
        ):
            app.logger.info(
                "[CACHE] HIT age=%.2fs ttl=%ds",
                cache_age,
                ttl,
            )

            return cached_flights, True, cache_age

    app.logger.info(
        "[CACHE] MISS"
    )

    # Only one request should refresh the cache at a time.
    with _db_lock:

        # Check again after obtaining the DB lock.
        # Another request may have refreshed the cache
        # while this request was waiting.
        now = time.monotonic()

        with _cache_lock:

            cached_flights = _flight_cache["flights"]
            cache_timestamp = _flight_cache["timestamp"]

            cache_age = now - cache_timestamp

            if (
                cached_flights is not None
                and cache_age < ttl
            ):
                app.logger.info(
                    "[CACHE] HIT after waiting "
                    "age=%.2fs ttl=%ds",
                    cache_age,
                    ttl,
                )

                return (
                    cached_flights,
                    True,
                    cache_age,
                )

        # -------------------------------------------------
        # CACHE REALLY IS EXPIRED
        # -------------------------------------------------

        global _db_connection

        for attempt in range(1, 3):

            try:
                connection = _get_connection(config)

                flights = _execute_flights_query(
                    connection,
                    config["table_name"],
                )

                break

            except Exception:
                app.logger.exception(
                    "[DBX] Query attempt %d failed",
                    attempt,
                )

                _close_connection()

                if attempt == 2:
                    raise

                app.logger.warning(
                    "[DBX] Reconnecting and retrying"
                )

        # -------------------------------------------------
        # UPDATE CACHE
        # -------------------------------------------------

        with _cache_lock:

            _flight_cache["flights"] = flights
            _flight_cache["timestamp"] = (
                time.monotonic()
            )

        app.logger.info(
            "[CACHE] Updated with %d flights",
            len(flights),
        )

        return flights, False, 0.0


# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.get("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "flightTrackerService",
    }), 200


@app.get("/getFlights")
def get_flights():

    request_start = time.monotonic()

    app.logger.info(
        "[API] /getFlights request received"
    )

    try:
        config = _load_config()

        flights, cached, cache_age = (
            _get_flights(config)
        )

    except Exception as exc:

        app.logger.exception(
            "[API] /getFlights failed: %s",
            type(exc).__name__,
        )

        return jsonify({
            "error": "Failed to retrieve flights",
            "exception_type": type(exc).__name__,
        }), 503

    elapsed = time.monotonic() - request_start

    app.logger.info(
        "[API] Returning %d flights "
        "cached=%s after %.3fs",
        len(flights),
        cached,
        elapsed,
    )

    return jsonify({
        "count": len(flights),

        # Useful while developing.
        # You can remove these later.
        "cached": cached,
        "cache_age_seconds": round(
            cache_age,
            2,
        ),

        "flights": flights,
    }), 200


# ---------------------------------------------------------
# CLEANUP
# ---------------------------------------------------------

atexit.register(_close_connection)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )

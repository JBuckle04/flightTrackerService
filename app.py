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


# One reusable Databricks connection per application process.
_db_connection = None

# Prevent simultaneous requests from using the same connection.
_db_lock = threading.Lock()


def _load_config():
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    table_name = os.getenv(
        "DATABRICKS_FLIGHTS_TABLE",
        "workspace.default.resolved_flights_v2",
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
            f"Missing Databricks configuration: "
            f"{', '.join(missing_values)}"
        )

    # The table name is inserted into the SQL statement, so only permit
    # valid one-part, two-part or three-part identifiers.
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
    }


def _create_connection(config):
    connect_start = time.monotonic()

    server_hostname = config["server_hostname"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]

    app.logger.info(
        "[DBX] Opening connection to host=%s",
        server_hostname,
    )

    def credential_provider():
        sdk_config = Config(
            host=f"https://{server_hostname}",
            client_id=client_id,
            client_secret=client_secret,
        )

        return oauth_service_principal(sdk_config)

    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=config["http_path"],
        credentials_provider=credential_provider,
    )

    app.logger.info(
        "[DBX] Connection opened in %.2fs",
        time.monotonic() - connect_start,
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
            "[DBX] Error while closing connection"
        )

    finally:
        _db_connection = None


def _execute_flights_query(connection, table_name):
    cursor = None

    try:
        cursor = connection.cursor()

        query = f"""
            SELECT *
            FROM {table_name}
        """

        execute_start = time.monotonic()

        app.logger.info(
            "[DBX] Executing query against table=%s",
            table_name,
        )

        cursor.execute(query)

        app.logger.info(
            "[DBX] Query execution completed in %.2fs",
            time.monotonic() - execute_start,
        )

        fetch_start = time.monotonic()

        rows = cursor.fetchall()
        columns = [
            column[0]
            for column in cursor.description
        ]

        app.logger.info(
            "[DBX] Fetched %d rows in %.2fs",
            len(rows),
            time.monotonic() - fetch_start,
        )

        return [
            dict(zip(columns, row))
            for row in rows
        ]

    finally:
        if cursor is not None:
            try:
                cursor.close()

            except Exception:
                app.logger.exception(
                    "[DBX] Error while closing cursor"
                )


def _query_flights():
    request_start = time.monotonic()
    config = _load_config()

    app.logger.info(
        "[DBX] Configuration loaded: "
        "host=%s table=%s",
        config["server_hostname"],
        config["table_name"],
    )

    # A single shared connection is protected from concurrent cursor use.
    with _db_lock:
        for attempt in range(1, 3):
            try:
                connection = _get_connection(config)

                flights = _execute_flights_query(
                    connection=connection,
                    table_name=config["table_name"],
                )

                app.logger.info(
                    "[DBX] Database operation completed in %.2fs",
                    time.monotonic() - request_start,
                )

                return flights

            except Exception:
                app.logger.exception(
                    "[DBX] Query attempt %d failed",
                    attempt,
                )

                # Discard a potentially stale or broken session.
                _close_connection()

                if attempt == 2:
                    raise

                app.logger.warning(
                    "[DBX] Reconnecting and retrying once"
                )

    # Defensive fallback; execution should never reach this point.
    raise RuntimeError(
        "Databricks query failed unexpectedly."
    )


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
        flights = _query_flights()

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
        "[API] Returning %d flights after %.2fs",
        len(flights),
        elapsed,
    )

    return jsonify({
        "count": len(flights),
        "query_seconds": round(elapsed, 3),
        "flights": flights,
    }), 200


# Close the connection cleanly if the worker process exits.
atexit.register(_close_connection)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )

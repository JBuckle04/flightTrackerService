import os
import re
import time
import logging

from flask import Flask, jsonify

app = Flask(__name__)

# Ensure logs appear in Azure App Service Log Stream
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)


@app.get("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "flightTrackerService"
    }), 200


def _query_flights():
    start = time.monotonic()

    # ---------------------------------------------------------
    # LOAD CONFIG
    # ---------------------------------------------------------

    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    table_name = os.getenv("DATABRICKS_FLIGHTS_TABLE", "flights")

    # Log whether variables exist, but NEVER log secrets
    config_status = {
        "DATABRICKS_HOST": bool(server_hostname),
        "DATABRICKS_HTTP_PATH": bool(http_path),
        "DATABRICKS_CLIENT_ID": bool(client_id),
        "DATABRICKS_CLIENT_SECRET": bool(client_secret),
        "DATABRICKS_FLIGHTS_TABLE": table_name,
    }

    app.logger.info(
        "[DBX] Configuration loaded: %s",
        config_status
    )

    if not all([
        server_hostname,
        http_path,
        client_id,
        client_secret
    ]):
        raise ValueError(
            "Missing Databricks configuration. Set "
            "DATABRICKS_HOST, "
            "DATABRICKS_HTTP_PATH, "
            "DATABRICKS_CLIENT_ID, and "
            "DATABRICKS_CLIENT_SECRET."
        )

    # Prevent SQL injection through configurable table name
    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}",
        table_name
    ):
        raise ValueError(
            "Invalid DATABRICKS_FLIGHTS_TABLE value."
        )

    # ---------------------------------------------------------
    # IMPORT DATABRICKS
    # ---------------------------------------------------------

    app.logger.info(
        "[DBX] Importing Databricks libraries"
    )

    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    app.logger.info(
        "[DBX] Databricks libraries imported after %.2fs",
        time.monotonic() - start
    )

    # ---------------------------------------------------------
    # SERVICE PRINCIPAL AUTH
    # ---------------------------------------------------------

    app.logger.info(
        "[DBX] Configuring service principal OAuth"
    )

    def credential_provider():
        config = Config(
            host=f"https://{server_hostname}",
            client_id=client_id,
            client_secret=client_secret,
        )

        return oauth_service_principal(config)

    # ---------------------------------------------------------
    # CONNECT
    # ---------------------------------------------------------

    connection = None
    cursor = None

    connect_start = time.monotonic()

    app.logger.info(
        "[DBX] Attempting connection to host=%s",
        server_hostname
    )

    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            credentials_provider=credential_provider,
        )

    except Exception:
        app.logger.exception(
            "[DBX] CONNECTION FAILED after %.2fs",
            time.monotonic() - connect_start
        )
        raise

    app.logger.info(
        "[DBX] CONNECTED successfully in %.2fs",
        time.monotonic() - connect_start
    )

    # ---------------------------------------------------------
    # QUERY
    # ---------------------------------------------------------

    try:
        cursor_start = time.monotonic()

        app.logger.info(
            "[DBX] Creating cursor"
        )

        cursor = connection.cursor()

        app.logger.info(
            "[DBX] Cursor created in %.2fs",
            time.monotonic() - cursor_start
        )

        query = f"SELECT * FROM {table_name}"

        query_start = time.monotonic()

        app.logger.info(
            "[DBX] Executing query against table=%s",
            table_name
        )

        try:
            cursor.execute(query)

        except Exception:
            app.logger.exception(
                "[DBX] QUERY FAILED after %.2fs",
                time.monotonic() - query_start
            )
            raise

        app.logger.info(
            "[DBX] Query executed successfully in %.2fs",
            time.monotonic() - query_start
        )

        # -----------------------------------------------------
        # FETCH RESULTS
        # -----------------------------------------------------

        fetch_start = time.monotonic()

        app.logger.info(
            "[DBX] Fetching rows"
        )

        rows = cursor.fetchall()

        app.logger.info(
            "[DBX] Fetched %d rows in %.2fs",
            len(rows),
            time.monotonic() - fetch_start
        )

        columns = [
            column[0]
            for column in cursor.description
        ]

        flights = [
            dict(zip(columns, row))
            for row in rows
        ]

        app.logger.info(
            "[DBX] Converted %d rows to dictionaries",
            len(flights)
        )

        return flights

    finally:

        # -----------------------------------------------------
        # CLEANUP
        # -----------------------------------------------------

        if cursor is not None:
            try:
                cursor.close()

                app.logger.info(
                    "[DBX] Cursor closed"
                )

            except Exception:
                app.logger.exception(
                    "[DBX] Error closing cursor"
                )

        if connection is not None:
            try:
                connection.close()

                app.logger.info(
                    "[DBX] Connection closed"
                )

            except Exception:
                app.logger.exception(
                    "[DBX] Error closing connection"
                )

        app.logger.info(
            "[DBX] Total DB operation: %.2fs",
            time.monotonic() - start
        )


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
            type(exc).__name__
        )

        return jsonify({
            "error": "Failed to retrieve flights",
            "exception_type": type(exc).__name__,
        }), 500

    app.logger.info(
        "[API] /getFlights returning %d flights after %.2fs",
        len(flights),
        time.monotonic() - request_start
    )

    return jsonify({
        "count": len(flights),
        "flights": flights
    }), 200


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000"))
    )

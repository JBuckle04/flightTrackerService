import os
import re

from flask import Flask, jsonify

app = Flask(__name__)


@app.get("/")
def health() -> tuple:
    return jsonify({"status": "ok", "service": "flightTrackerService"}), 200


def _query_flights():
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    table_name = os.getenv("DATABRICKS_FLIGHTS_TABLE", "flights")

    if not server_hostname or not http_path or not client_id or not client_secret:
        raise ValueError(
            "Missing Databricks configuration. Set DATABRICKS_HOST, "
            "DATABRICKS_HTTP_PATH, DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET."
        )
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}", table_name):
        raise ValueError("Invalid DATABRICKS_FLIGHTS_TABLE value.")

    from databricks import sql

    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        client_id=client_id,
        client_secret=client_secret,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]

    return [dict(zip(columns, row)) for row in rows]


@app.get("/getFlights")
def get_flights() -> tuple:
    try:
        flights = _query_flights()
    except Exception:
        app.logger.exception("Failed to query flights from Databricks")
        return jsonify({"error": "Failed to retrieve flights"}), 500

    return jsonify({"flights": flights}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

import unittest
from unittest.mock import patch

from app import app


class AppTestCase(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_root_route_returns_service_status(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {"status": "ok", "service": "flightTrackerService"},
        )

    @patch("app._query_flights")
    def test_get_flights_returns_query_results(self, query_flights_mock):
        query_flights_mock.return_value = [
            {"flight_number": "AB123", "status": "On Time"},
            {"flight_number": "CD456", "status": "Delayed"},
        ]

        response = self.client.get("/getFlights")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "flights": [
                    {"flight_number": "AB123", "status": "On Time"},
                    {"flight_number": "CD456", "status": "Delayed"},
                ]
            },
        )

    @patch("app._query_flights")
    def test_get_flights_returns_error_when_query_fails(self, query_flights_mock):
        query_flights_mock.side_effect = Exception("Databricks unavailable")

        response = self.client.get("/getFlights")

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.get_json(), {"error": "Databricks unavailable"})


if __name__ == "__main__":
    unittest.main()

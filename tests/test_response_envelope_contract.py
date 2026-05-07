import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from features.operations import tools as operations_tools
from features.performance import tools as performance_tools
from features.sales import tools as sales_tools


class TestMCPResponseEnvelopeContract(unittest.TestCase):
    @patch("features.sales.use_cases.bookings.finance_summary")
    @patch("features.sales.use_cases.bookings.tuple_to_dataframe")
    @patch("features.sales.use_cases.bookings.save_result_to_json")
    @patch("features.sales.use_cases.bookings.NetSuiteConnection")
    @patch("features.sales.use_cases.bookings.get_bookings_data")
    def test_sales_get_bookings_envelope_contract(
        self,
        mock_get_bookings_data,
        mock_netsuite_connection,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_finance_summary,
    ):
        mock_get_bookings_data.return_value = "SELECT bookings"
        mock_save_result_to_json.return_value = {"filename": "bookings_data.json", "path": "/tmp/bookings_data.json"}

        columns = ["quote_id", "amount", "inside_sales"]
        rows = [("Q-1", 100.0, "JUAN")]

        ns_client = MagicMock()
        ns_client.execute_query.return_value = (columns, rows)
        managed_ctx = MagicMock()
        managed_ctx.__enter__.return_value = ns_client
        managed_ctx.__exit__.return_value = False
        mock_netsuite_connection.return_value.managed.return_value = managed_ctx

        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_finance_summary.return_value = {
            "bookings_total": 100.0,
            "quotes_count": 1,
            "full_data_reference": "internal-only",
        }

        response = sales_tools.get_bookings(
            initial_date="2026-01-01",
            final_date="2026-01-31",
            customer_name="acme",
            inside_sales="juan",
        )

        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts"})
        self.assertEqual(response["meta"]["tool_name"], "get_bookings")
        self.assertEqual(
            set(response["meta"]["filters"].keys()),
            {"initial_date", "final_date", "customer_name", "inside_sales"},
        )
        self.assertEqual(response["meta"]["filters"]["customer_name"], "ACME")
        self.assertEqual(response["meta"]["filters"]["inside_sales"], "JUAN")

        self.assertIn("dataset", response["artifacts"])
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "bookings_data.json")

        self.assertIn("bookings_total", response["kpi_metrics"])
        self.assertIn("quotes_count", response["kpi_metrics"])
        self.assertNotIn("full_data_reference", response["kpi_metrics"])

    @patch("features.operations.use_cases.otd.on_time_delivery_summary")
    @patch("features.operations.use_cases.otd.tuple_to_dataframe")
    @patch("features.operations.use_cases.otd.save_result_to_json")
    @patch("features.operations.use_cases.otd.execute_pg_query_dev")
    @patch("features.operations.use_cases.otd.get_on_time_delivery")
    def test_operations_get_otd_indicators_envelope_contract(
        self,
        mock_get_on_time_delivery,
        mock_execute_pg_query_dev,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_on_time_delivery_summary,
    ):
        mock_get_on_time_delivery.return_value = "SELECT otd"
        mock_save_result_to_json.return_value = {"filename": "otd_data.json", "path": "/tmp/otd_data.json"}

        columns = ["so_number", "is_on_time"]
        rows = [("SO-1", True)]
        mock_execute_pg_query_dev.return_value = (columns, rows)

        df = pd.DataFrame(rows, columns=columns)
        mock_tuple_to_dataframe.return_value = df
        mock_on_time_delivery_summary.return_value = {"otd_percent": 100.0, "late_orders": 0}

        response = operations_tools.get_otd_indicators(
            initial_date="2026-02-01",
            final_date="2026-02-28",
            so_number="SO-1",
        )

        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts"})
        self.assertEqual(response["meta"]["tool_name"], "get_otd_indicators")
        self.assertEqual(
            set(response["meta"]["filters"].keys()),
            {"initial_date", "final_date", "so_number"},
        )
        self.assertEqual(response["meta"]["filters"]["so_number"], "SO-1")

        self.assertIn("dataset", response["artifacts"])
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "otd_data.json")

        self.assertIn("otd_percent", response["kpi_metrics"])
        self.assertIn("late_orders", response["kpi_metrics"])
        self.assertIn("so_details", response["kpi_metrics"])
        self.assertIsInstance(response["kpi_metrics"]["so_details"], list)

    @patch("features.performance.use_cases.scorecard.execute_pg_query_dev")
    @patch("features.performance.use_cases.scorecard.get_scorecard_by_is_year")
    @patch("features.performance.use_cases.scorecard.get_scorecard_by_is_daily")
    @patch("features.performance.use_cases.scorecard.get_scorecard_by_is_month")
    def test_performance_get_scorecard_by_is_envelope_contract(
        self,
        mock_get_month,
        mock_get_daily,
        mock_get_year,
        mock_execute_pg_query_dev,
    ):
        mock_get_month.return_value = "SELECT month"
        mock_get_daily.return_value = "SELECT day"
        mock_get_year.return_value = "SELECT year"

        monthly = (["inside_sales", "score"], [("JUAN", 10)])
        daily = (["inside_sales", "score"], [("JUAN", 1), ("JUAN", 2)])
        yearly = (["inside_sales", "score"], [("JUAN", 100)])
        mock_execute_pg_query_dev.side_effect = [monthly, daily, yearly]

        response = performance_tools.get_scorecard_by_is(inside_sales="juan")

        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts", "details"})
        self.assertEqual(response["meta"]["tool_name"], "get_scorecard_by_is")
        self.assertEqual(set(response["meta"]["filters"].keys()), {"inside_sales"})
        self.assertEqual(response["meta"]["filters"]["inside_sales"], "juan")

        self.assertIn("dataset", response["artifacts"])
        self.assertIsNone(response["artifacts"]["dataset"])

        self.assertIn("monthly_scorecard", response["kpi_metrics"])
        self.assertIn("daily_scorecard", response["kpi_metrics"])
        self.assertIn("yearly_scorecard", response["kpi_metrics"])

        self.assertIn("row_counts", response["details"])
        self.assertEqual(response["details"]["row_counts"]["monthly"], 1)
        self.assertEqual(response["details"]["row_counts"]["daily"], 2)
        self.assertEqual(response["details"]["row_counts"]["yearly"], 1)


if __name__ == "__main__":
    unittest.main()

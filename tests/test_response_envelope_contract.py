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
        mock_get_bookings_data.return_value = (
            "SELECT bookings",
            ["2026-01-01", "2026-01-31", "%ACME%", "%JUAN%", "Pending Fulfillment"],
        )
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
            status="Pending Fulfillment",
        )

        ns_client.execute_query.assert_called_once_with(
            "SELECT bookings",
            ["2026-01-01", "2026-01-31", "%ACME%", "%JUAN%", "Pending Fulfillment"],
        )

        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts"})
        self.assertEqual(response["meta"]["tool_name"], "get_bookings")
        self.assertEqual(
            set(response["meta"]["filters"].keys()),
            {"initial_date", "final_date", "customer_name", "inside_sales", "status"},
        )
        self.assertEqual(response["meta"]["filters"]["customer_name"], "ACME")
        self.assertEqual(response["meta"]["filters"]["inside_sales"], "JUAN")
        self.assertEqual(response["meta"]["filters"]["status"], "Pending Fulfillment")

        self.assertIn("dataset", response["artifacts"])
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "bookings_data.json")

        self.assertIn("bookings_total", response["kpi_metrics"])
        self.assertIn("quotes_count", response["kpi_metrics"])
        self.assertNotIn("full_data_reference", response["kpi_metrics"])

    def test_sales_get_bookings_rejects_invalid_status(self):
        with self.assertRaisesRegex(ValueError, "Invalid status 'Closed'"):
            sales_tools.get_bookings(status="Closed")

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
        mock_get_on_time_delivery.return_value = ("SELECT otd", ["2026-02-01", "2026-02-28", "SO-1"])
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

        mock_execute_pg_query_dev.assert_called_once_with(
            "SELECT otd",
            ["2026-02-01", "2026-02-28", "SO-1"],
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

    @patch("features.operations.use_cases.purchase_orders.build_vendors_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.execute_pg_query")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query_pg")
    def test_operations_get_purchase_orders_envelope_contract(
        self,
        mock_get_purchase_orders_query_pg,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_vendors_analysis,
    ):
        mock_get_purchase_orders_query_pg.return_value = (
            "SELECT po",
            ["2026-05-01", "2026-05-11", "Vendor A", "Pending Receipt", "Brand X"],
        )
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}

        columns = ["po_id", "amount_usd"]
        rows = [(1, 100.0)]
        mock_execute_pg_query.return_value = (columns, rows)

        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_vendors_analysis.return_value = {
            "topic": "vendors",
            "overview": {"total_purchase_orders": 1},
            "full_data_reference": "internal-only",
        }

        response = operations_tools.get_purchase_orders(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor="Vendor A",
            status="Pending Receipt",
            brand="Brand X",
            topic="vendors",
        )

        mock_execute_pg_query.assert_called_once_with(
            "SELECT po",
            ["2026-05-01", "2026-05-11", "Vendor A", "Pending Receipt", "Brand X"],
        )
        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts"})
        self.assertEqual(response["meta"]["tool_name"], "get_purchase_orders")
        self.assertEqual(response["meta"]["source_systems"], ["postgresql"])
        self.assertEqual(
            set(response["meta"]["filters"].keys()),
            {"initial_date", "final_date", "vendor", "status", "brand", "topic"},
        )
        self.assertEqual(response["meta"]["filters"]["topic"], "vendors")
        self.assertNotIn("full_data_reference", response["kpi_metrics"])

    @patch("features.operations.use_cases.purchase_orders.build_walle_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.execute_pg_query")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_walle_query_pg")
    def test_operations_get_purchase_orders_walle_envelope_contract(
        self,
        mock_get_purchase_orders_walle_query_pg,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_walle_analysis,
    ):
        mock_get_purchase_orders_walle_query_pg.return_value = (
            "SELECT po_walle",
            ["2026-05-01", "2026-05-11", "%Vendor A%"],
        )
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}

        columns = ["po_id", "email_count"]
        rows = [(1, 3)]
        mock_execute_pg_query.return_value = (columns, rows)
        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_walle_analysis.return_value = {
            "topic": "walle",
            "overview": {"total_purchase_orders": 1},
            "full_data_reference": "internal-only",
        }

        response = operations_tools.get_purchase_orders(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor="Vendor A",
            topic="walle",
        )

        mock_execute_pg_query.assert_called_once_with(
            "SELECT po_walle",
            ["2026-05-01", "2026-05-11", "%Vendor A%"],
        )
        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts"})
        self.assertEqual(response["meta"]["tool_name"], "get_purchase_orders")
        self.assertEqual(response["meta"]["source_systems"], ["postgresql"])
        self.assertEqual(response["meta"]["filters"]["topic"], "walle")
        self.assertNotIn("full_data_reference", response["kpi_metrics"])

    @patch("features.operations.use_cases.walle_usage.build_walle_usage_metrics")
    @patch("features.operations.use_cases.walle_usage.save_dataset_manifest")
    @patch("features.operations.use_cases.walle_usage.save_result_to_json")
    @patch("features.operations.use_cases.walle_usage.execute_pg_query")
    def test_operations_get_walle_usage_envelope_contract(
        self,
        mock_execute_pg_query_dev,
        mock_save_result_to_json,
        mock_save_dataset_manifest,
        mock_build_walle_usage_metrics,
    ):
        mock_execute_pg_query_dev.side_effect = [
            (["po_name", "email_id", "email_type"], [("PO-1", "E-1", "follow_up")]),
            (["po_name", "email_id", "confidence"], [("PO-1", "E-1", 0.9)]),
            (["po_name", "createdAt", "updatedAt"], [("PO-1", "2026-05-01T09:00:00", "2026-05-01T10:00:00")]),
            (["endpoint", "status_code", "duration_ms"], [("/walle/analyze", 200, 150)]),
        ]
        mock_save_result_to_json.side_effect = [
            {"filename": "summarized.json"},
            {"filename": "analyzed.json"},
            {"filename": "actions.json"},
            {"filename": "events.json"},
        ]
        mock_save_dataset_manifest.return_value = {"filename": "walle_manifest.json"}
        mock_build_walle_usage_metrics.return_value = {
            "overview": {"processed_po_count": 1},
            "email_summary": {},
            "email_analysis": {},
            "actions": {},
            "api_usage": {},
        }

        response = operations_tools.get_walle_usage(
            initial_date="2026-05-01",
            final_date="2026-05-12",
            po_name="PO-1",
            limit=25,
        )

        self.assertEqual(set(response.keys()), {"meta", "kpi_metrics", "artifacts", "details"})
        self.assertEqual(response["meta"]["tool_name"], "get_walle_usage")
        self.assertEqual(response["meta"]["source_systems"], ["postgresql"])
        self.assertEqual(
            set(response["meta"]["filters"].keys()),
            {"initial_date", "final_date", "po_name", "limit"},
        )
        self.assertEqual(response["meta"]["filters"]["po_name"], "PO-1")
        self.assertEqual(response["meta"]["filters"]["limit"], 25)
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "walle_manifest.json")
        self.assertIn("overview", response["kpi_metrics"])
        self.assertIn("dataset_references", response["details"])
        self.assertEqual(
            response["details"]["dataset_references"],
            {
                "summarized_emails": "summarized.json",
                "analyzed_emails": "analyzed.json",
                "action_suggestions": "actions.json",
                "event_log": "events.json",
            },
        )
        self.assertEqual(
            response["details"]["row_counts"],
            {
                "summarized_emails": 1,
                "analyzed_emails": 1,
                "action_suggestions": 1,
                "event_log": 1,
            },
        )
        self.assertEqual(response["details"]["sample_limit"], 25)
        self.assertIn("Request the dataset", response["details"]["note"])

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
        mock_get_month.return_value = ("SELECT month", ["juan"])
        mock_get_daily.return_value = ("SELECT day", ["juan"])
        mock_get_year.return_value = ("SELECT year", ["juan"])

        monthly = (["inside_sales", "score"], [("JUAN", 10)])
        daily = (["inside_sales", "score"], [("JUAN", 1), ("JUAN", 2)])
        yearly = (["inside_sales", "score"], [("JUAN", 100)])
        mock_execute_pg_query_dev.side_effect = [monthly, daily, yearly]

        response = performance_tools.get_scorecard_by_is(inside_sales="juan")

        self.assertEqual(
            mock_execute_pg_query_dev.call_args_list,
            [
                unittest.mock.call("SELECT month", ["juan"]),
                unittest.mock.call("SELECT day", ["juan"]),
                unittest.mock.call("SELECT year", ["juan"]),
            ],
        )

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

    @patch("features.sales.use_cases.opportunities.save_result_to_json")
    @patch("features.sales.use_cases.opportunities.NetSuiteConnection")
    @patch("features.sales.use_cases.opportunities.get_opportunities_data")
    def test_sales_get_opportunities_empty_result_contract(
        self,
        mock_get_opportunities_data,
        mock_netsuite_connection,
        mock_save_result_to_json,
    ):
        mock_get_opportunities_data.return_value = (
            "SELECT opportunities",
            ["2025-05-01", "2025-05-08", "%Daniel Jaramillo%", "%Pueblo Viejo%"],
        )
        mock_save_result_to_json.return_value = {"filename": "opportunity_by_is.json", "path": "/tmp/opportunity_by_is.json"}

        columns = ["id", "op_number", "tran_date", "expected_close_date", "customer", "subsidiary", "status", "inside_sales"]
        rows = []

        ns_client = MagicMock()
        ns_client.execute_query.return_value = (columns, rows)
        managed_ctx = MagicMock()
        managed_ctx.__enter__.return_value = ns_client
        managed_ctx.__exit__.return_value = False
        mock_netsuite_connection.return_value.managed.return_value = managed_ctx

        response = sales_tools.get_opportunities(
            initial_date="2025-05-01",
            final_date="2025-05-08",
            inside_sales="Daniel Jaramillo",
            customer_name="Pueblo Viejo",
        )

        ns_client.execute_query.assert_called_once_with(
            "SELECT opportunities",
            ["2025-05-01", "2025-05-08", "%Daniel Jaramillo%", "%Pueblo Viejo%"],
        )

        self.assertEqual(response["meta"]["tool_name"], "get_opportunities")
        self.assertEqual(response["kpi_metrics"]["period"]["start_date"], None)
        self.assertEqual(response["kpi_metrics"]["period"]["end_date"], None)
        self.assertEqual(response["kpi_metrics"]["overview"]["total_opportunities"], 0)
        self.assertEqual(response["kpi_metrics"]["overview"]["total_unique_customers"], 0)
        self.assertEqual(response["kpi_metrics"]["distribution"]["inside_sales"], [])
        self.assertEqual(response["kpi_metrics"]["distribution"]["status"], [])
        self.assertEqual(response["kpi_metrics"]["overdue_in_progress"], [])


if __name__ == "__main__":
    unittest.main()

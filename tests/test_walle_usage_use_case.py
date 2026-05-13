import unittest
from unittest.mock import patch

from features.operations.use_cases import walle_usage


class TestWalleUsageUseCase(unittest.TestCase):
    @patch("features.operations.use_cases.walle_usage.build_walle_usage_metrics")
    @patch("features.operations.use_cases.walle_usage.save_dataset_manifest")
    @patch("features.operations.use_cases.walle_usage.save_result_to_json")
    @patch("features.operations.use_cases.walle_usage.execute_pg_query")
    def test_execute_applies_defaults_and_returns_dataset_references(
        self,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_save_dataset_manifest,
        mock_build_walle_usage_metrics,
    ):
        columns = ["po_name", "email_id"]
        rows = [("PO-1", "E-1"), ("PO-2", "E-2")]
        event_columns = ["endpoint", "status_code", "duration_ms"]
        event_rows = [("/walle/analyze", 200, 100)]
        mock_execute_pg_query.side_effect = [
            (columns, rows),
            (columns, rows),
            (["po_name", "createdAt", "updatedAt"], [("PO-1", "2026-05-03T08:00:00", "2026-05-03T08:10:00")]),
            (event_columns, event_rows),
        ]
        mock_save_result_to_json.side_effect = [
            {"filename": "summarized.json"},
            {"filename": "analyzed.json"},
            {"filename": "actions.json"},
            {"filename": "events.json"},
        ]
        mock_save_dataset_manifest.return_value = {"filename": "manifest.json"}
        mock_build_walle_usage_metrics.return_value = {"overview": {"processed_po_count": 2}}

        with patch("features.operations.use_cases.walle_usage.get_month_start_and_today", return_value=("2026-05-01", "2026-05-12")):
            response = walle_usage.execute(limit=600)

        self.assertEqual(mock_execute_pg_query.call_args_list[0].args[1], ["2026-05-01", "2026-05-12"])
        self.assertEqual(mock_execute_pg_query.call_args_list[1].args[1], ["2026-05-01", "2026-05-12"])
        self.assertEqual(mock_execute_pg_query.call_args_list[2].args[1], ["2026-05-01", "2026-05-12"])
        self.assertEqual(mock_execute_pg_query.call_args_list[3].args[1], ["2026-05-01", "2026-05-12"])
        self.assertEqual(response["meta"]["tool_name"], "get_walle_usage")
        self.assertEqual(response["meta"]["filters"]["limit"], 500)
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "manifest.json")
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
                "summarized_emails": 2,
                "analyzed_emails": 2,
                "action_suggestions": 1,
                "event_log": 1,
            },
        )
        self.assertEqual(response["details"]["sample_limit"], 500)
        self.assertIn("Request the dataset", response["details"]["note"])
        self.assertEqual(
            response["details"]["row_counts"],
            {
                "summarized_emails": 2,
                "analyzed_emails": 2,
                "action_suggestions": 1,
                "event_log": 1,
            },
        )
        self.assertEqual(response["details"]["sample_limit"], 500)
        self.assertIn("Request the dataset", response["details"]["note"])

    @patch("features.operations.use_cases.walle_usage.build_walle_usage_metrics")
    @patch("features.operations.use_cases.walle_usage.save_dataset_manifest")
    @patch("features.operations.use_cases.walle_usage.save_result_to_json")
    @patch("features.operations.use_cases.walle_usage.execute_pg_query")
    def test_execute_uses_po_filter_for_po_queries_and_dates_for_event_log(
        self,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_save_dataset_manifest,
        mock_build_walle_usage_metrics,
    ):
        mock_execute_pg_query.side_effect = [([], []), ([], []), ([], []), ([], [])]
        mock_save_result_to_json.side_effect = [
            {"filename": "summarized.json"},
            {"filename": "analyzed.json"},
            {"filename": "actions.json"},
            {"filename": "events.json"},
        ]
        mock_save_dataset_manifest.return_value = {"filename": "manifest.json"}
        mock_build_walle_usage_metrics.return_value = {"overview": {}}

        response = walle_usage.execute(initial_date="2026-05-01", final_date="2026-05-10", po_name="  PO-777  ")

        self.assertEqual(mock_execute_pg_query.call_args_list[0].args[1], ["PO-777"])
        self.assertEqual(mock_execute_pg_query.call_args_list[1].args[1], ["PO-777"])
        self.assertEqual(mock_execute_pg_query.call_args_list[2].args[1], ["PO-777"])
        self.assertEqual(mock_execute_pg_query.call_args_list[3].args[1], ["2026-05-01", "2026-05-10"])
        self.assertEqual(response["meta"]["filters"]["po_name"], "PO-777")

    def test_execute_validates_date_order(self):
        with self.assertRaisesRegex(ValueError, "initial_date must be less than or equal to final_date"):
            walle_usage.execute(initial_date="2026-05-10", final_date="2026-05-01")

    def test_execute_validates_date_format(self):
        with self.assertRaisesRegex(ValueError, "Expected YYYY-MM-DD"):
            walle_usage.execute(initial_date="05/01/2026", final_date="2026-05-10")

    def test_execute_validates_limit(self):
        with self.assertRaisesRegex(ValueError, "limit must be greater than 0"):
            walle_usage.execute(limit=0)

    @patch("features.operations.use_cases.walle_usage.execute_pg_query", side_effect=Exception("db down"))
    def test_execute_wraps_postgres_errors(self, _mock_execute_pg_query):
        with self.assertRaisesRegex(RuntimeError, "Failed to retrieve Walle usage metrics from PostgreSQL"):
            walle_usage.execute(initial_date="2026-05-01", final_date="2026-05-10")


if __name__ == "__main__":
    unittest.main()

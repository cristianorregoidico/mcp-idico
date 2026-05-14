import unittest
from unittest.mock import patch

from features.operations.use_cases import idra_usage


class TestIdraUsageUseCase(unittest.TestCase):
    @patch("features.operations.use_cases.idra_usage.build_idra_usage_metrics")
    @patch("features.operations.use_cases.idra_usage.save_result_to_json")
    @patch("features.operations.use_cases.idra_usage.execute_pg_query")
    def test_execute_returns_envelope_and_dataset_reference(
        self,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_build_idra_usage_metrics,
    ):
        mock_execute_pg_query.return_value = (
            ["tool_name", "username", "response", "duration_ms", "created_at"],
            [("get_quotes", "ana@idico.com", "ERROR: boom", 120, "2026-05-01T10:00:00+00:00")],
        )
        mock_save_result_to_json.return_value = {"filename": "idra_usage.json"}
        mock_build_idra_usage_metrics.return_value = {"summary": {"total_calls": 1}}

        response = idra_usage.execute("2026-05-01", "2026-05-10")

        self.assertEqual(mock_execute_pg_query.call_args.args[1], ["2026-05-01", "2026-05-11"])
        self.assertEqual(response["meta"]["tool_name"], "get_idra_usage")
        self.assertEqual(response["meta"]["filters"], {"initial_date": "2026-05-01", "final_date": "2026-05-10"})
        self.assertEqual(response["meta"]["row_count"], 1)
        self.assertEqual(response["meta"]["column_count"], 5)
        self.assertEqual(response["artifacts"]["dataset"]["filename"], "idra_usage.json")
        self.assertEqual(response["details"]["dataset_reference"], "idra_usage.json")
        self.assertEqual(response["details"]["row_count"], 1)

    def test_execute_validates_date_order(self):
        with self.assertRaisesRegex(ValueError, "initial_date must be less than or equal to final_date"):
            idra_usage.execute("2026-05-10", "2026-05-01")

    def test_execute_validates_date_format(self):
        with self.assertRaisesRegex(ValueError, "Expected YYYY-MM-DD"):
            idra_usage.execute("05/01/2026", "2026-05-10")

    @patch("features.operations.use_cases.idra_usage.execute_pg_query", side_effect=Exception("db down"))
    def test_execute_wraps_postgres_errors(self, _mock_execute_pg_query):
        with self.assertRaisesRegex(RuntimeError, "Failed to retrieve IDRA usage metrics from PostgreSQL"):
            idra_usage.execute("2026-05-01", "2026-05-10")


if __name__ == "__main__":
    unittest.main()

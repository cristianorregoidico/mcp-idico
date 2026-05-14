import unittest

import pandas as pd

from features.operations.domain.idra_usage import build_idra_usage_metrics, classify_response


class TestIdraUsageDomain(unittest.TestCase):
    def test_classify_response_detects_error_text(self):
        classified = classify_response("ERROR: Could not establish NetSuite connection")

        self.assertEqual(classified["response_kind"], "error_text")
        self.assertEqual(classified["execution_status"], "error")
        self.assertEqual(classified["error_message"], "Could not establish NetSuite connection")

    def test_classify_response_detects_python_dict_text(self):
        classified = classify_response("{'meta': {'tool_name': 'get_quotes'}, 'kpi_metrics': {'overview': {}}}")

        self.assertEqual(classified["response_kind"], "python_dict_text")
        self.assertEqual(classified["execution_status"], "success")
        self.assertIsNone(classified["error_message"])

    def test_classify_response_marks_unparseable_text_as_unknown(self):
        classified = classify_response("{'meta': {'tool_name': 'get_quotes'}")

        self.assertEqual(classified["response_kind"], "unknown_text")
        self.assertEqual(classified["execution_status"], "unknown")

    def test_build_idra_usage_metrics_computes_expected_kpis(self):
        df = pd.DataFrame(
            [
                {
                    "tool_name": "get_quotes",
                    "username": "ana@idico.com",
                    "response": "{'meta': {'tool_name': 'get_quotes'}, 'kpi_metrics': {'overview': {}}}",
                    "duration_ms": 120,
                    "created_at": "2026-05-01T10:00:00+00:00",
                },
                {
                    "tool_name": "get_quotes",
                    "username": "ana@idico.com",
                    "response": "ERROR: Both customer_name and brand must be provided.",
                    "duration_ms": 240,
                    "created_at": "2026-05-01T11:00:00+00:00",
                },
                {
                    "tool_name": "get_bookings",
                    "username": "bob@idico.com",
                    "response": "{'meta': {'tool_name': 'get_bookings'}, 'kpi_metrics': {'overview': {}}}",
                    "duration_ms": 60,
                    "created_at": "2026-05-02T10:30:00+00:00",
                },
                {
                    "tool_name": "get_bookings",
                    "username": "bob@idico.com",
                    "response": "{'meta': {'tool_name': 'get_bookings'}",
                    "duration_ms": 400,
                    "created_at": "2026-05-02T12:30:00+00:00",
                },
            ]
        )

        metrics = build_idra_usage_metrics(df)

        self.assertEqual(metrics["summary"]["total_calls"], 4)
        self.assertEqual(metrics["summary"]["unique_tools"], 2)
        self.assertEqual(metrics["summary"]["unique_users"], 2)
        self.assertEqual(metrics["summary"]["avg_calls_per_day"], 2.0)

        self.assertEqual(metrics["quality"]["success_count"], 2)
        self.assertEqual(metrics["quality"]["error_count"], 1)
        self.assertEqual(metrics["quality"]["unknown_count"], 1)
        self.assertAlmostEqual(metrics["quality"]["error_rate_pct"], 25.0)
        self.assertEqual(metrics["quality"]["top_error_messages"][0]["error_message"], "Both customer_name and brand must be provided.")

        self.assertAlmostEqual(metrics["performance"]["avg_duration_ms"], 205.0)
        self.assertAlmostEqual(metrics["performance"]["median_duration_ms"], 180.0)
        self.assertAlmostEqual(metrics["performance"]["p95_duration_ms"], 376.0)
        self.assertAlmostEqual(metrics["performance"]["p99_duration_ms"], 395.2)

        self.assertEqual(metrics["tools"]["top_used"][0]["tool_name"], "get_bookings")
        self.assertEqual(metrics["tools"]["top_used"][0]["call_count"], 2)
        self.assertEqual(metrics["users"]["top_active"][0]["username"], "ana@idico.com")
        self.assertEqual(len(metrics["usage_patterns"]["calls_by_day"]), 2)
        self.assertEqual(metrics["usage_patterns"]["peak_day"]["date"], "2026-05-02")
        self.assertEqual(metrics["usage_patterns"]["peak_hour"]["hour"], 10)

    def test_build_idra_usage_metrics_handles_empty_dataframe(self):
        metrics = build_idra_usage_metrics(pd.DataFrame())

        self.assertEqual(metrics["summary"], {"total_calls": 0, "unique_tools": 0, "unique_users": 0, "avg_calls_per_day": 0.0})
        self.assertEqual(metrics["tools"]["top_used"], [])
        self.assertEqual(metrics["users"]["top_active"], [])
        self.assertEqual(metrics["performance"], {"avg_duration_ms": 0.0, "median_duration_ms": 0.0, "p95_duration_ms": 0.0, "p99_duration_ms": 0.0})
        self.assertEqual(metrics["quality"]["success_count"], 0)
        self.assertEqual(metrics["quality"]["error_count"], 0)
        self.assertEqual(metrics["quality"]["unknown_count"], 0)


if __name__ == "__main__":
    unittest.main()

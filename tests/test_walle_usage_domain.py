import unittest

import pandas as pd

from features.operations.domain.walle_usage import build_walle_usage_metrics


class TestWalleUsageDomain(unittest.TestCase):
    def test_build_walle_usage_metrics_computes_expected_kpis(self):
        summarized_emails_df = pd.DataFrame(
            [
                {
                    "po_name": "PO-1",
                    "po_status": "Open",
                    "vendor_name": "Vendor A",
                    "email_id": "E-1",
                    "email_date": "2026-05-01T10:00:00",
                    "email_type": "follow_up",
                },
                {
                    "po_name": "PO-2",
                    "po_status": "Closed",
                    "vendor_name": "Vendor B",
                    "email_id": "E-2",
                    "email_date": "2026-05-03T09:00:00",
                    "email_type": "status_update",
                },
                {
                    "po_name": "PO-1",
                    "po_status": "Open",
                    "vendor_name": "Vendor A",
                    "email_id": "E-1",
                    "email_date": "2026-05-01T10:00:00",
                    "email_type": "follow_up",
                },
            ]
        )

        analyzed_emails_df = pd.DataFrame(
            [
                {"po_name": "PO-1", "email_id": "E-1", "confidence": 0.8, "scenario": "delay", "version": "v1"},
                {"po_name": "PO-3", "email_id": "E-3", "confidence": 0.6, "scenario": "eta", "version": "v2"},
            ]
        )

        action_suggestions_df = pd.DataFrame(
            [
                {"po_name": "PO-1", "createdAt": "2026-05-04T08:00:00", "updatedAt": "2026-05-04T09:00:00"},
                {"po_name": "PO-1", "createdAt": "2026-05-05T08:00:00", "updatedAt": "2026-05-05T09:30:00"},
                {"po_name": "PO-2", "createdAt": "2026-05-06T07:00:00", "updatedAt": "2026-05-06T07:15:00"},
            ]
        )

        event_log_df = pd.DataFrame(
            [
                {"endpoint": "/walle/analyze", "status_code": 200, "duration_ms": 100, "createdAt": "2026-05-01T10:00:00"},
                {"endpoint": "/walle/analyze", "status_code": 500, "duration_ms": 200, "createdAt": "2026-05-01T10:05:00"},
                {"endpoint": "/walle/summary", "status_code": 201, "duration_ms": 300, "createdAt": "2026-05-01T10:10:00"},
            ]
        )

        metrics = build_walle_usage_metrics(
            summarized_emails_df=summarized_emails_df,
            analyzed_emails_df=analyzed_emails_df,
            action_suggestions_df=action_suggestions_df,
            event_log_df=event_log_df,
        )

        self.assertEqual(metrics["overview"]["processed_po_count"], 3)
        self.assertEqual(metrics["overview"]["summarized_email_count"], 2)
        self.assertEqual(metrics["overview"]["analyzed_email_count"], 2)
        self.assertEqual(metrics["overview"]["suggested_action_records"], 3)
        self.assertEqual(metrics["overview"]["walle_event_count"], 3)

        self.assertEqual(metrics["email_summary"]["latest_email_date"], "2026-05-03T09:00:00")
        self.assertEqual(metrics["email_summary"]["emails_by_type"][0]["email_type"], "follow_up")
        self.assertEqual(metrics["email_summary"]["emails_by_type"][0]["count"], 2)

        self.assertAlmostEqual(metrics["email_analysis"]["avg_confidence"], 0.7)
        self.assertEqual({row["scenario"] for row in metrics["email_analysis"]["scenario_distribution"]}, {"delay", "eta"})
        self.assertEqual({row["version"] for row in metrics["email_analysis"]["version_distribution"]}, {"v1", "v2"})

        self.assertEqual(metrics["actions"]["total_action_records"], 3)
        self.assertEqual(metrics["actions"]["actions_by_po"][0]["po_name"], "PO-1")
        self.assertEqual(metrics["actions"]["actions_by_po"][0]["action_count"], 2)
        self.assertEqual(metrics["actions"]["latest_created_at"], "2026-05-06T07:00:00")
        self.assertEqual(metrics["actions"]["latest_updated_at"], "2026-05-06T07:15:00")

        self.assertEqual(metrics["api_usage"]["error_count"], 1)
        self.assertAlmostEqual(metrics["api_usage"]["error_rate_pct"], 33.33333333333333)
        self.assertAlmostEqual(metrics["api_usage"]["duration_ms"]["avg"], 200.0)
        self.assertAlmostEqual(metrics["api_usage"]["duration_ms"]["p50"], 200.0)
        self.assertAlmostEqual(metrics["api_usage"]["duration_ms"]["p95"], 290.0)
        self.assertAlmostEqual(metrics["api_usage"]["duration_ms"]["max"], 300.0)
        self.assertEqual(len(metrics["api_usage"]["events_by_endpoint"]), 2)

    def test_build_walle_usage_metrics_handles_empty_dataframes(self):
        empty = pd.DataFrame()

        metrics = build_walle_usage_metrics(
            summarized_emails_df=empty,
            analyzed_emails_df=empty,
            action_suggestions_df=empty,
            event_log_df=empty,
        )

        self.assertEqual(
            metrics["overview"],
            {
                "processed_po_count": 0,
                "summarized_email_count": 0,
                "analyzed_email_count": 0,
                "suggested_action_records": 0,
                "walle_event_count": 0,
            },
        )
        self.assertEqual(metrics["email_summary"]["emails_by_type"], [])
        self.assertIsNone(metrics["email_summary"]["latest_email_date"])
        self.assertEqual(metrics["email_analysis"]["avg_confidence"], 0.0)
        self.assertEqual(metrics["actions"]["actions_by_po"], [])
        self.assertEqual(metrics["actions"]["total_action_records"], 0)
        self.assertEqual(metrics["api_usage"]["events_by_endpoint"], [])
        self.assertEqual(metrics["api_usage"]["status_code_distribution"], [])
        self.assertEqual(metrics["api_usage"]["duration_ms"], {"avg": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0})
        self.assertEqual(metrics["api_usage"]["error_count"], 0)
        self.assertEqual(metrics["api_usage"]["error_rate_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()

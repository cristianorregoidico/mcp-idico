import unittest
from unittest.mock import patch
from datetime import datetime, timezone

import pandas as pd

from features.operations.domain.purchase_orders import (
    build_items_analysis,
    build_vendors_analysis,
    build_walle_analysis,
    normalize_topic,
)
from features.operations.use_cases import purchase_orders


class TestPurchaseOrdersAnalysis(unittest.TestCase):
    @patch("features.operations.use_cases.purchase_orders.build_vendors_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.execute_pg_query")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query_pg")
    @patch("features.operations.use_cases.purchase_orders.get_month_start_and_today")
    def test_use_case_defaults_missing_dates_and_topic_defaults_to_vendors(
        self,
        mock_get_month_start_and_today,
        mock_get_purchase_orders_query_pg,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_vendors_analysis,
    ):
        mock_get_month_start_and_today.return_value = ("2026-05-01", "2026-05-11")
        mock_get_purchase_orders_query_pg.return_value = ("SELECT po", ["2026-05-01", "2026-05-11"])
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}

        columns = ["po_id"]
        rows = [(1,)]
        mock_execute_pg_query.return_value = (columns, rows)

        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_vendors_analysis.return_value = {"topic": "vendors", "overview": {"total_purchase_orders": 1}}

        response = purchase_orders.execute(initial_date=None, final_date="2026-05-20", topic="")

        mock_get_purchase_orders_query_pg.assert_called_once_with(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor=None,
            status=None,
            brand=None,
        )
        self.assertEqual(response["meta"]["filters"]["topic"], "vendors")

    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query_pg")
    def test_use_case_invalid_status_fails_before_query(self, mock_get_purchase_orders_query_pg):
        with self.assertRaises(ValueError):
            purchase_orders.execute(
                initial_date="2026-05-01",
                final_date="2026-05-11",
                status="Invalid",
            )

        mock_get_purchase_orders_query_pg.assert_not_called()

    @patch("features.operations.use_cases.purchase_orders.build_items_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.execute_pg_query")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query_pg")
    def test_use_case_normalizes_vendor_status_brand_for_items_topic(
        self,
        mock_get_purchase_orders_query_pg,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_items_analysis,
    ):
        mock_get_purchase_orders_query_pg.return_value = ("SELECT po", ["2026-05-01", "2026-05-11", "%ACME%", "Pending Receipt", "%Cisco%"])
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}
        columns = ["po_id"]
        rows = [(1,)]
        mock_execute_pg_query.return_value = (columns, rows)
        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_items_analysis.return_value = {"topic": "items", "overview": {"total_lines": 1}}

        response = purchase_orders.execute(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor="  ACME  ",
            status=" Pending Receipt ",
            brand="  Cisco ",
            topic="items",
        )

        mock_get_purchase_orders_query_pg.assert_called_once_with(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor="ACME",
            status="Pending Receipt",
            brand="Cisco",
        )
        self.assertEqual(response["meta"]["filters"]["vendor"], "ACME")
        self.assertEqual(response["meta"]["filters"]["status"], "Pending Receipt")
        self.assertEqual(response["meta"]["filters"]["brand"], "Cisco")
        self.assertEqual(response["meta"]["filters"]["topic"], "items")

    def test_invalid_topic_raises_controlled_error(self):
        with self.assertRaises(ValueError):
            normalize_topic("not-valid")

    @patch("features.operations.use_cases.purchase_orders.build_walle_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.execute_pg_query")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_walle_query_pg")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query_pg")
    def test_use_case_walle_topic_uses_dedicated_query_and_builder(
        self,
        mock_get_purchase_orders_query_pg,
        mock_get_purchase_orders_walle_query_pg,
        mock_execute_pg_query,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_walle_analysis,
    ):
        mock_get_purchase_orders_walle_query_pg.return_value = ("SELECT po_walle", ["2026-05-01", "2026-05-11", "%Cisco%"])
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}
        columns = ["po_id", "email_count"]
        rows = [(1, 3)]
        mock_execute_pg_query.return_value = (columns, rows)
        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_walle_analysis.return_value = {"topic": "walle", "overview": {"total_purchase_orders": 1}}

        response = purchase_orders.execute(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            brand=" Cisco ",
            topic="walle",
        )

        mock_get_purchase_orders_walle_query_pg.assert_called_once_with(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor=None,
            status=None,
            brand="Cisco",
        )
        mock_get_purchase_orders_query_pg.assert_not_called()
        self.assertEqual(response["meta"]["filters"]["topic"], "walle")

    def test_walle_topic_analysis_uses_processed_manual_non_null_and_workflow_priority(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 1,
                    "po_number": "PO-1",
                    "po_date": "2026-05-01",
                    "today": "2026-05-11",
                    "status": "Pending Receipt",
                    "vendor": "Vendor A",
                    "amount_usd": 100.0,
                    "email_count": 10,
                    "inbound_email_count": 6,
                    "outbound_email_count": 4,
                    "ai_processed_email_count": 4,
                    "last_email_date": "2026-05-10",
                    "suggestion_row_count": 3,
                    "processed_suggestion_count": 2,
                    "unprocessed_suggestion_count": 1,
                    "suggestion_true_count": 1,
                    "suggestion_false_count": 1,
                    "total_actions_suggested": 5,
                    "avg_actions_per_suggestion": 1.67,
                    "last_suggestion_at": "2026-05-09",
                    "finance_request_count": 1,
                    "finance_pending_count": 1,
                    "finance_completed_count": 0,
                    "finance_cancelled_count": 0,
                    "finance_executed_count": 0,
                    "finance_with_support_files_count": 1,
                    "finance_rejection_event_count": 0,
                    "finance_anulation_event_count": 0,
                    "anticipo_count": 1,
                    "gasto_importacion_count": 0,
                    "last_finance_updated_at": "2026-05-08",
                },
                {
                    "po_id": 2,
                    "po_number": "PO-2",
                    "po_date": "2026-05-02",
                    "today": "2026-05-11",
                    "status": "Pending Receipt",
                    "vendor": "Vendor B",
                    "amount_usd": 50.0,
                    "email_count": 2,
                    "inbound_email_count": 2,
                    "outbound_email_count": 0,
                    "ai_processed_email_count": 1,
                    "last_email_date": "2026-05-07",
                    "suggestion_row_count": 1,
                    "processed_suggestion_count": 1,
                    "unprocessed_suggestion_count": 0,
                    "suggestion_true_count": 0,
                    "suggestion_false_count": 1,
                    "total_actions_suggested": 2,
                    "avg_actions_per_suggestion": 2.0,
                    "last_suggestion_at": "2026-05-08",
                    "finance_request_count": 1,
                    "finance_pending_count": 0,
                    "finance_completed_count": 1,
                    "finance_cancelled_count": 0,
                    "finance_executed_count": 1,
                    "finance_with_support_files_count": 1,
                    "finance_rejection_event_count": 0,
                    "finance_anulation_event_count": 0,
                    "anticipo_count": 0,
                    "gasto_importacion_count": 1,
                    "last_finance_updated_at": "2026-05-06",
                },
                {
                    "po_id": 3,
                    "po_number": "PO-3",
                    "po_date": "2026-05-03",
                    "today": "2026-05-11",
                    "status": "Pending Receipt",
                    "vendor": "Vendor C",
                    "amount_usd": 75.0,
                    "email_count": 1,
                    "inbound_email_count": 1,
                    "outbound_email_count": 0,
                    "ai_processed_email_count": 0,
                    "last_email_date": "2026-05-04",
                    "suggestion_row_count": 1,
                    "processed_suggestion_count": 0,
                    "unprocessed_suggestion_count": 1,
                    "suggestion_true_count": 0,
                    "suggestion_false_count": 0,
                    "total_actions_suggested": 1,
                    "avg_actions_per_suggestion": 1.0,
                    "last_suggestion_at": "2026-05-05",
                    "finance_request_count": 1,
                    "finance_pending_count": 0,
                    "finance_completed_count": 0,
                    "finance_cancelled_count": 0,
                    "finance_executed_count": 0,
                    "finance_with_support_files_count": 1,
                    "finance_rejection_event_count": 1,
                    "finance_anulation_event_count": 0,
                    "anticipo_count": 1,
                    "gasto_importacion_count": 0,
                    "last_finance_updated_at": "2026-05-05",
                },
            ]
        )

        result = build_walle_analysis(df)

        self.assertEqual(result["topic"], "walle")
        self.assertEqual(result["overview"]["total_purchase_orders"], 3)
        self.assertEqual(result["overview"]["purchase_orders_with_processed_suggestions"], 2)
        self.assertEqual(result["suggestion_metrics"]["processed_suggestion_count"], 3)
        self.assertEqual(result["suggestion_metrics"]["suggestion_false_count"], 2)
        self.assertAlmostEqual(result["email_metrics"]["ai_processed_email_ratio"], 5 / 13)

        workflow = {row["workflow_stage"]: row["po_count"] for row in result["workflow_stage_breakdown"]}
        self.assertEqual(workflow["finance_pending"], 1)
        self.assertEqual(workflow["finance_completed"], 1)
        self.assertEqual(workflow["finance_exception"], 1)

        pending_pos = result["top_purchase_orders_with_pending_finance"]
        self.assertEqual(pending_pos[0]["po_number"], "PO-1")

    def test_walle_topic_analysis_normalizes_tz_aware_and_naive_dates(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 1,
                    "po_number": "PO-1",
                    "po_date": "2026-05-01",
                    "today": "2026-05-11",
                    "status": "Pending Receipt",
                    "vendor": "Vendor A",
                    "amount_usd": 100.0,
                    "email_count": 1,
                    "inbound_email_count": 1,
                    "outbound_email_count": 0,
                    "ai_processed_email_count": 1,
                    "last_email_date": datetime(2026, 5, 10, 10, 0, tzinfo=timezone.utc),
                    "suggestion_row_count": 1,
                    "processed_suggestion_count": 1,
                    "unprocessed_suggestion_count": 0,
                    "suggestion_true_count": 1,
                    "suggestion_false_count": 0,
                    "total_actions_suggested": 1,
                    "avg_actions_per_suggestion": 1.0,
                    "last_suggestion_at": datetime(2026, 5, 9, 10, 0, tzinfo=timezone.utc),
                    "finance_request_count": 1,
                    "finance_pending_count": 0,
                    "finance_completed_count": 1,
                    "finance_cancelled_count": 0,
                    "finance_executed_count": 1,
                    "finance_with_support_files_count": 1,
                    "finance_rejection_event_count": 0,
                    "finance_anulation_event_count": 0,
                    "anticipo_count": 1,
                    "gasto_importacion_count": 0,
                    "last_finance_updated_at": datetime(2026, 5, 8, 10, 0),
                }
            ]
        )

        result = build_walle_analysis(df)

        self.assertEqual(result["topic"], "walle")
        self.assertEqual(result["workflow_stage_breakdown"][0]["workflow_stage"], "finance_completed")

    def test_items_topic_analysis_uses_line_receipt_score_and_keeps_pending_quantity_secondary(self):
        df = pd.DataFrame(
            [
                {
                    "item": "Item-1",
                    "brand": "A",
                    "product_group": "G1",
                    "vendor": "V1",
                    "customer": "C1",
                    "quantity": 10,
                    "quantity_received": 4,
                    "line_amount": 100,
                }
            ]
        )
        result = build_items_analysis(df)

        self.assertEqual(result["topic"], "items")
        self.assertEqual(result["overview"]["total_pending_quantity"], 6.0)
        self.assertEqual(result["receipt_analysis"]["avg_line_receipt_score"], 0.4)
        self.assertEqual(result["receipt_analysis"]["avg_line_receipt_score_pct"], 40.0)
        self.assertEqual(result["receipt_analysis"]["secondary_pending_quantity"], 6.0)

    def test_items_topic_analysis_averages_receipt_score_by_line_not_by_quantity(self):
        df = pd.DataFrame(
            [
                {
                    "item": "Big line",
                    "brand": "A",
                    "product_group": "G1",
                    "vendor": "V1",
                    "customer": "C1",
                    "quantity": 10000,
                    "quantity_received": 0,
                    "line_amount": 100,
                },
                {
                    "item": "Partial line",
                    "brand": "A",
                    "product_group": "G1",
                    "vendor": "V1",
                    "customer": "C1",
                    "quantity": 100,
                    "quantity_received": 30,
                    "line_amount": 50,
                },
                {
                    "item": "Full line",
                    "brand": "B",
                    "product_group": "G2",
                    "vendor": "V2",
                    "customer": "C2",
                    "quantity": 20,
                    "quantity_received": 20,
                    "line_amount": 75,
                },
            ]
        )

        result = build_items_analysis(df)

        self.assertAlmostEqual(result["receipt_analysis"]["avg_line_receipt_score"], (0.0 + 0.3 + 1.0) / 3)
        self.assertEqual(result["receipt_analysis"]["not_received_lines"], 1)
        self.assertEqual(result["receipt_analysis"]["partially_received_lines"], 1)
        self.assertEqual(result["receipt_analysis"]["fully_received_lines"], 1)
        brand_a = next(row for row in result["brand_analysis"]["metrics"] if row["brand"] == "A")
        self.assertAlmostEqual(brand_a["avg_line_receipt_score"], 0.15)
        self.assertAlmostEqual(brand_a["avg_line_receipt_score_pct"], 15.0)

    def test_vendors_analysis_deduplicates_po_amount(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 1,
                    "po_period": "2026-05",
                    "status": "Pending Receipt",
                    "approval_status": "Approved",
                    "expediting_status": "Open",
                    "payment_status": "Pending",
                    "vendor": "Vendor A",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "CO",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 100.0,
                    "receive_by": "2026-05-20",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
                {
                    "po_id": 1,
                    "po_period": "2026-05",
                    "status": "Pending Receipt",
                    "approval_status": "Approved",
                    "expediting_status": "Open",
                    "payment_status": "Pending",
                    "vendor": "Vendor A",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "CO",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 100.0,
                    "receive_by": "2026-05-20",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
            ]
        )
        result = build_vendors_analysis(df)

        self.assertEqual(result["overview"]["total_purchase_orders"], 1)
        self.assertEqual(result["overview"]["total_amount_usd"], 100.0)

    def test_vendors_delivery_analysis_excludes_fully_billed_and_pending_bill(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 1,
                    "po_period": "2026-05",
                    "status": "Pending Receipt",
                    "approval_status": "Approved",
                    "expediting_status": "Open",
                    "payment_status": "Pending",
                    "vendor": "Vendor A",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "CO",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 100.0,
                    "receive_by": "2026-05-08",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
                {
                    "po_id": 2,
                    "po_period": "2026-05",
                    "status": "Fully Billed",
                    "approval_status": "Approved",
                    "expediting_status": "Closed",
                    "payment_status": "Paid",
                    "vendor": "Vendor B",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "US",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 200.0,
                    "receive_by": "2026-05-01",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
                {
                    "po_id": 3,
                    "po_period": "2026-05",
                    "status": "Pending Bill",
                    "approval_status": "Approved",
                    "expediting_status": "Closed",
                    "payment_status": "Pending Bill",
                    "vendor": "Vendor C",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "MX",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 300.0,
                    "receive_by": "2026-05-02",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
            ]
        )

        result = build_vendors_analysis(df)

        self.assertEqual(result["delivery_analysis"]["overdue_pos"], 1)
        self.assertEqual(result["delivery_analysis"]["overdue_amount_usd"], 100.0)
        self.assertEqual(result["delivery_analysis"]["avg_overdue_days"], 3.0)

    def test_vendors_delivery_analysis_uses_last_informed_delivery_date_when_later(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 20,
                    "po_period": "2026-05",
                    "status": "Pending Receipt",
                    "approval_status": "Approved",
                    "expediting_status": "Open",
                    "payment_status": "Pending",
                    "vendor": "Vendor A",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "CO",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 100.0,
                    "receive_by": "2026-05-01",
                    "new_receive_by": "2026-05-03",
                    "last_est_delivery_date_informed": "2026-05-15",
                    "today": "2026-05-11",
                }
            ]
        )

        result = build_vendors_analysis(df)

        self.assertEqual(result["delivery_analysis"]["overdue_pos"], 0)
        self.assertEqual(result["delivery_analysis"]["due_this_week"], 1)
        self.assertEqual(result["delivery_analysis"]["avg_days_to_receive"], 4.0)

    def test_vendors_closed_delivery_analysis_detects_only_positive_delay_on_closed_pos(self):
        df = pd.DataFrame(
            [
                {
                    "po_id": 10,
                    "po_period": "2026-05",
                    "status": "Fully Billed",
                    "approval_status": "Approved",
                    "expediting_status": "Closed",
                    "payment_status": "Paid",
                    "vendor": "Vendor A",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "CO",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 100.0,
                    "receive_by": "2026-05-01",
                    "new_receive_by": "2026-05-04",
                    "last_est_delivery_date_informed": "2026-05-06",
                    "today": "2026-05-11",
                },
                {
                    "po_id": 11,
                    "po_period": "2026-05",
                    "status": "Pending Bill",
                    "approval_status": "Approved",
                    "expediting_status": "Closed",
                    "payment_status": "Pending Bill",
                    "vendor": "Vendor B",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "US",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 200.0,
                    "receive_by": "2026-05-02",
                    "new_receive_by": None,
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
                {
                    "po_id": 12,
                    "po_period": "2026-05",
                    "status": "Fully Billed",
                    "approval_status": "Approved",
                    "expediting_status": "Closed",
                    "payment_status": "Paid",
                    "vendor": "Vendor C",
                    "terms": "Net 30",
                    "incoterms": "FOB",
                    "vendor_country": "MX",
                    "subsidiary": "Sub 1",
                    "customer": "Cust",
                    "amount_usd": 300.0,
                    "receive_by": "2026-05-10",
                    "new_receive_by": "2026-05-08",
                    "last_est_delivery_date_informed": None,
                    "today": "2026-05-11",
                },
            ]
        )

        result = build_vendors_analysis(df)

        self.assertEqual(result["closed_delivery_analysis"]["closed_pos_count"], 3)
        self.assertEqual(result["closed_delivery_analysis"]["closed_pos_with_delay"], 1)
        self.assertEqual(result["closed_delivery_analysis"]["closed_pos_with_delay_pct"], 33.33333333333333)
        self.assertEqual(result["closed_delivery_analysis"]["avg_delay_days"], 5.0)
        self.assertEqual(result["closed_delivery_analysis"]["delayed_amount_usd"], 100.0)


if __name__ == "__main__":
    unittest.main()

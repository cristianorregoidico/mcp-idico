import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from features.operations.domain.purchase_orders import build_items_analysis, build_vendors_analysis, normalize_topic
from features.operations.use_cases import purchase_orders


class TestPurchaseOrdersAnalysis(unittest.TestCase):
    @patch("features.operations.use_cases.purchase_orders.build_vendors_analysis")
    @patch("features.operations.use_cases.purchase_orders.tuple_to_dataframe")
    @patch("features.operations.use_cases.purchase_orders.save_result_to_json")
    @patch("features.operations.use_cases.purchase_orders.NetSuiteConnection")
    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query")
    @patch("features.operations.use_cases.purchase_orders.get_month_start_and_today")
    def test_use_case_defaults_missing_dates_and_topic_defaults_to_vendors(
        self,
        mock_get_month_start_and_today,
        mock_get_purchase_orders_query,
        mock_netsuite_connection,
        mock_save_result_to_json,
        mock_tuple_to_dataframe,
        mock_build_vendors_analysis,
    ):
        mock_get_month_start_and_today.return_value = ("2026-05-01", "2026-05-11")
        mock_get_purchase_orders_query.return_value = ("SELECT po", ["2026-05-01", "2026-05-11"])
        mock_save_result_to_json.return_value = {"filename": "purchase_orders_data.json", "path": "/tmp/purchase_orders_data.json"}

        columns = ["po_id"]
        rows = [(1,)]
        ns_client = MagicMock()
        ns_client.execute_query.return_value = (columns, rows)
        managed_ctx = MagicMock()
        managed_ctx.__enter__.return_value = ns_client
        managed_ctx.__exit__.return_value = False
        mock_netsuite_connection.return_value.managed.return_value = managed_ctx

        mock_tuple_to_dataframe.return_value = pd.DataFrame(rows, columns=columns)
        mock_build_vendors_analysis.return_value = {"topic": "vendors", "overview": {"total_purchase_orders": 1}}

        response = purchase_orders.execute(initial_date=None, final_date="2026-05-20", topic="")

        mock_get_purchase_orders_query.assert_called_once_with(
            initial_date="2026-05-01",
            final_date="2026-05-11",
            vendor=None,
            status=None,
            brand=None,
        )
        self.assertEqual(response["meta"]["filters"]["topic"], "vendors")

    @patch("features.operations.use_cases.purchase_orders.get_purchase_orders_query")
    def test_use_case_invalid_status_fails_before_query(self, mock_get_purchase_orders_query):
        with self.assertRaises(ValueError):
            purchase_orders.execute(
                initial_date="2026-05-01",
                final_date="2026-05-11",
                status="Invalid",
            )

        mock_get_purchase_orders_query.assert_not_called()

    def test_invalid_topic_raises_controlled_error(self):
        with self.assertRaises(ValueError):
            normalize_topic("not-valid")

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

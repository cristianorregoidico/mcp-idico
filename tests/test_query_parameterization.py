import unittest

from features.operations.queries.guides import get_helga_guides_query
from features.operations.queries.otd import get_on_time_delivery
from features.operations.queries.purchase_orders import get_purchase_orders_query, get_purchase_orders_query_pg
from features.operations.queries.walle_usage import (
    get_action_suggestions_query,
    get_analyzed_emails_query,
    get_summarized_emails_query,
    get_walle_event_log_query,
)
from features.operations.queries.idra_usage import get_idra_usage_query
from features.sales.queries.activity import get_calls_summary
from features.sales.queries.quotes import get_quotes_by_inside
from features.sales.queries.vendor_recommendation import (
    get_customer_country,
    get_vendors_country_brand,
    get_vendors_customer_brand,
)


class TestQueryParameterization(unittest.TestCase):
    def test_netsuite_quotes_without_optional_filters(self):
        sql, params = get_quotes_by_inside("2026-01-01", "2026-01-31", "", "")

        self.assertIn("BETWEEN ? AND ?", sql)
        self.assertNotIn("customer_name", sql)
        self.assertNotIn("inside_sales", sql)
        self.assertEqual(params, ["2026-01-01", "2026-01-31"])

    def test_postgres_guides_default_status_behavior_preserved(self):
        sql, params = get_helga_guides_query(po=None, status=None, service=None)

        self.assertIn("gh.status_envio <> 'GUIA ENTREGADA'", sql)
        self.assertEqual(params, [])

    def test_postgres_calls_summary_dynamic_filters(self):
        sql, params = get_calls_summary(
            start_date="2026-03-01",
            final_date="2026-03-31",
            customer_name="ACME",
            organizer="Juan",
            subject="Pricing",
        )

        self.assertIn("activity_date >= %s", sql)
        self.assertIn("account ILIKE %s", sql)
        self.assertIn("organizer ILIKE %s", sql)
        self.assertIn("subject ILIKE %s", sql)
        self.assertEqual(params, ["2026-03-01", "2026-03-31", "%ACME%", "%Juan%", "%Pricing%"])

    def test_postgres_otd_uses_date_casted_placeholders(self):
        sql, params = get_on_time_delivery("2026-02-01", "2026-02-28")

        self.assertIn("BETWEEN %s::date AND %s::date", sql)
        self.assertEqual(params, ["2026-02-01", "2026-02-28"])

    def test_purchase_orders_only_required_filters(self):
        sql, params = get_purchase_orders_query("2026-04-01", "2026-04-30")

        self.assertIn("TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?", sql)
        self.assertNotIn("BUILTIN.DF(t.entity) LIKE '%' || ? || '%'", sql)
        self.assertNotIn("ts.name LIKE '%' || ? || '%'", sql)
        self.assertNotIn("BUILTIN.DF(i.custitem13) LIKE '%' || ? || '%'", sql)
        self.assertEqual(params, ["2026-04-01", "2026-04-30"])

    def test_purchase_orders_dynamic_filters_and_non_interpolated_values(self):
        vendor = "ACME Vendor"
        status = "Pending Receipt"
        brand = "Brand-X"
        sql, params = get_purchase_orders_query("2026-04-01", "2026-04-30", vendor=vendor, status=status, brand=brand)

        self.assertIn("BUILTIN.DF(t.entity) LIKE '%' || UPPER(?) || '%'", sql)
        self.assertIn("ts.name LIKE '%' || ? || '%'", sql)
        self.assertIn("BUILTIN.DF(i.custitem13) LIKE '%' || UPPER(?) || '%'", sql)
        self.assertEqual(params, ["2026-04-01", "2026-04-30", vendor, status, brand])

        self.assertNotIn(vendor, sql)
        self.assertNotIn(status, sql)
        self.assertNotIn(brand, sql)

    def test_purchase_orders_pg_only_required_filters(self):
        sql, params = get_purchase_orders_query_pg("2026-04-01", "2026-04-30")

        self.assertIn("po.po_date BETWEEN %s::date AND %s::date", sql)
        self.assertIn("po.po_status NOT IN ('Undefined', 'Closed', 'Planned')", sql)
        self.assertNotIn("vendor_name ILIKE", sql)
        self.assertNotIn("po_status ILIKE", sql)
        self.assertNotIn("i.brand ILIKE", sql)
        self.assertEqual(params, ["2026-04-01", "2026-04-30"])

    def test_purchase_orders_pg_dynamic_filters_and_non_interpolated_values(self):
        vendor = "ACME Vendor"
        status = "Pending Receipt"
        brand = "Brand-X"
        sql, params = get_purchase_orders_query_pg("2026-04-01", "2026-04-30", vendor=vendor, status=status, brand=brand)

        self.assertIn("po.vendor_name ILIKE '%' || %s || '%'", sql)
        self.assertIn("po.po_status ILIKE '%' || %s || '%'", sql)
        self.assertIn("i.brand ILIKE '%' || %s || '%'", sql)
        self.assertEqual(params, ["2026-04-01", "2026-04-30", vendor, status, brand])

        self.assertNotIn(vendor, sql)
        self.assertNotIn(status, sql)
        self.assertNotIn(brand, sql)

    def test_walle_summarized_emails_uses_po_filter_without_between(self):
        sql, params = get_summarized_emails_query(po_name="PO-123")

        self.assertIn("po.po_name = %s", sql)
        self.assertNotIn("poe.email_date::date BETWEEN %s AND %s", sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["PO-123"])

    def test_walle_summarized_emails_uses_date_range_without_po(self):
        sql, params = get_summarized_emails_query("2026-05-01", "2026-05-31")

        self.assertIn("poe.email_date::date BETWEEN %s AND %s", sql)
        self.assertNotIn("po.po_name = %s", sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["2026-05-01", "2026-05-31"])

    def test_walle_analyzed_emails_uses_po_filter_without_between(self):
        sql, params = get_analyzed_emails_query(po_name="PO-456")

        self.assertIn("po.po_name = %s", sql)
        self.assertNotIn('mea."createdAt"::date BETWEEN %s AND %s', sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["PO-456"])

    def test_walle_analyzed_emails_uses_date_range_without_po(self):
        sql, params = get_analyzed_emails_query("2026-05-01", "2026-05-31")

        self.assertIn('mea."createdAt"::date BETWEEN %s AND %s', sql)
        self.assertNotIn("po.po_name = %s", sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["2026-05-01", "2026-05-31"])

    def test_walle_action_suggestions_uses_po_filter_without_between(self):
        sql, params = get_action_suggestions_query(po_name="PO-789")

        self.assertIn("eas.manual IS NOT NULL", sql)
        self.assertIn("po.po_name = %s", sql)
        self.assertNotIn('eas."createdAt"::date BETWEEN %s AND %s', sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["PO-789"])

    def test_walle_action_suggestions_uses_date_range_without_po(self):
        sql, params = get_action_suggestions_query("2026-05-01", "2026-05-31")

        self.assertIn("eas.manual IS NOT NULL", sql)
        self.assertIn('eas."createdAt"::date BETWEEN %s AND %s', sql)
        self.assertNotIn("po.po_name = %s", sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["2026-05-01", "2026-05-31"])

    def test_walle_event_log_always_uses_date_range_without_limit(self):
        sql, params = get_walle_event_log_query("2026-05-01", "2026-05-31")

        self.assertIn('el."createdAt"::date BETWEEN %s AND %s', sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["2026-05-01", "2026-05-31"])

    def test_idra_usage_query_uses_exclusive_upper_bound_placeholders(self):
        sql, params = get_idra_usage_query("2026-05-01", "2026-05-11")

        self.assertIn("created_at >= %s", sql)
        self.assertIn("created_at < %s", sql)
        self.assertIn("username <> 'Not Identified'", sql)
        self.assertNotIn("LIMIT", sql.upper())
        self.assertEqual(params, ["2026-05-01", "2026-05-11"])

    def test_vendor_recommendation_customer_brand_uses_ilike_params(self):
        sql, params = get_vendors_customer_brand("ACME", "3M")

        self.assertIn("customer_name ILIKE %s", sql)
        self.assertIn("brand ILIKE %s", sql)
        self.assertEqual(params, ["%ACME%", "%3M%"])

    def test_vendor_recommendation_country_brand_uses_ilike_params(self):
        sql, params = get_vendors_country_brand("CO", "3M")

        self.assertIn("country ILIKE %s", sql)
        self.assertIn("brand ILIKE %s", sql)
        self.assertEqual(params, ["%CO%", "%3M%"])

    def test_vendor_recommendation_customer_country_uses_ilike_param(self):
        sql, params = get_customer_country("ACME")

        self.assertIn("customer_name ILIKE %s", sql)
        self.assertEqual(params, ["%ACME%"])


if __name__ == "__main__":
    unittest.main()

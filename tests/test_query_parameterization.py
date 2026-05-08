import unittest

from features.operations.queries.guides import get_helga_guides_query
from features.operations.queries.otd import get_on_time_delivery
from features.sales.queries.activity import get_calls_summary
from features.sales.queries.quotes import get_quotes_by_inside


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
        self.assertIn("UPPER(account) ILIKE '%' || UPPER(%s) || '%'", sql)
        self.assertIn("UPPER(organizer) ILIKE '%' || UPPER(%s) || '%'", sql)
        self.assertIn("UPPER(subject) ILIKE '%' || UPPER(%s) || '%'", sql)
        self.assertEqual(params, ["2026-03-01", "2026-03-31", "ACME", "Juan", "Pricing"])

    def test_postgres_otd_uses_date_casted_placeholders(self):
        sql, params = get_on_time_delivery("2026-02-01", "2026-02-28")

        self.assertIn("BETWEEN %s::date AND %s::date", sql)
        self.assertEqual(params, ["2026-02-01", "2026-02-28"])


if __name__ == "__main__":
    unittest.main()

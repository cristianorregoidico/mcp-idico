import unittest

import pandas as pd

from features.sales.domain.quotes import summarize_is_quotes


class TestQuotesDomain(unittest.TestCase):
    def test_summarize_is_quotes_includes_only_budget_metrics(self):
        df = pd.DataFrame(
            [
                {
                    "CreateDate": "2026-05-01",
                    "ExpirationDate": "2026-05-15",
                    "Status": "Open",
                    "approval_state": "Approved",
                    "idico_vendor": "Vendor A",
                    "InsideSale": "Juan Perez",
                    "QuoteNumber": "Q-1",
                    "only_budget": "T",
                    "Customer": "ACME",
                    "Subsidiary": "IDICO MX",
                    "IncoTerms": "FOB",
                    "Amount": 100.0,
                    "GrossMargin": 20.0,
                    "GrossMarginPct": 0.20,
                },
                {
                    "CreateDate": "2026-05-02",
                    "ExpirationDate": "2026-05-16",
                    "Status": "Closed",
                    "approval_state": "Unapproved",
                    "idico_vendor": "Vendor A",
                    "InsideSale": "Juan Perez",
                    "QuoteNumber": "Q-2",
                    "only_budget": "F",
                    "Customer": "Globex",
                    "Subsidiary": "IDICO MX",
                    "IncoTerms": "CIF",
                    "Amount": 200.0,
                    "GrossMargin": 60.0,
                    "GrossMarginPct": 0.30,
                },
                {
                    "CreateDate": "2026-05-03",
                    "ExpirationDate": "2026-05-17",
                    "Status": "Open",
                    "approval_state": None,
                    "idico_vendor": None,
                    "InsideSale": "Ana Diaz",
                    "QuoteNumber": "Q-3",
                    "only_budget": None,
                    "Customer": "Initech",
                    "Subsidiary": "IDICO US",
                    "IncoTerms": "EXW",
                    "Amount": 300.0,
                    "GrossMargin": 90.0,
                    "GrossMarginPct": 0.30,
                },
            ]
        )

        summary = summarize_is_quotes(df)

        general_total = summary["overview"]["general_total"]
        self.assertEqual(general_total["total_transactions"], 3)
        self.assertEqual(general_total["only_budget_quotes_count"], 1)
        self.assertAlmostEqual(general_total["only_budget_quotes_share"], 1 / 3)
        self.assertEqual(general_total["only_budget_amount"], 100.0)

        approval_distribution = {
            item["approval_state"]: item
            for item in summary["overview"]["approval_state_distribution"]
        }
        self.assertEqual(set(approval_distribution.keys()), {"Approved", "Unapproved", "Unknown"})
        self.assertEqual(approval_distribution["Approved"]["quotes_count"], 1)
        self.assertAlmostEqual(approval_distribution["Approved"]["quotes_share"], 1 / 3)
        self.assertEqual(approval_distribution["Approved"]["total_amount"], 100.0)
        self.assertEqual(approval_distribution["Unknown"]["total_amount"], 300.0)

        vendor_distribution = {
            item["idico_vendor"]: item
            for item in summary["overview"]["idico_vendor_distribution"]
        }
        self.assertEqual(set(vendor_distribution.keys()), {"Vendor A", "Unknown"})
        self.assertEqual(vendor_distribution["Vendor A"]["quotes_count"], 2)
        self.assertAlmostEqual(vendor_distribution["Vendor A"]["quotes_share"], 2 / 3)
        self.assertEqual(vendor_distribution["Vendor A"]["total_amount"], 300.0)
        self.assertEqual(vendor_distribution["Unknown"]["quotes_count"], 1)
        self.assertEqual(vendor_distribution["Unknown"]["total_amount"], 300.0)



if __name__ == "__main__":
    unittest.main()

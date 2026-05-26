import unittest

import pandas as pd

from features.sales.domain.items import summarize_sold_items


class TestSoldItemsGrossMargin(unittest.TestCase):
    def test_summarize_sold_items_subtracts_line_freight_from_gm(self):
        df = pd.DataFrame(
            [
                {
                    "customer": "Customer A",
                    "quote": "SO-1",
                    "status": "Pending Fulfillment",
                    "date": "2026-05-01",
                    "inside_sales": "Sales Rep",
                    "item": "ITEM-1",
                    "item_description": "Item 1",
                    "brand": "Brand A",
                    "product_group": "Group A",
                    "selected_vendor": "Vendor A",
                    "qty": 10,
                    "unit_price": 100,
                    "unit_cost": 70,
                    "gross_margin_pct": 0.30,
                    "inbound_freight_cost": 50,
                    "outbound_freight_cost": 30,
                }
            ]
        )

        summary = summarize_sold_items(df)

        self.assertEqual(summary["overview"]["total_sales"], 1000)
        self.assertEqual(summary["overview"]["total_cost"], 700)
        self.assertEqual(summary["overview"]["total_inbound_freight_cost"], 50)
        self.assertEqual(summary["overview"]["total_outbound_freight_cost"], 30)
        self.assertEqual(summary["overview"]["total_gross_margin"], 220)
        self.assertEqual(summary["overview"]["average_gross_margin_pct"], 0.22)

        top_item = summary["top_items"]["by_margin_amount"][0]
        self.assertEqual(top_item["total_gm"], 220)
        self.assertEqual(top_item["avg_gm_pct"], 0.22)

    def test_summarize_sold_items_accepts_zero_line_freight_costs(self):
        df = pd.DataFrame(
            [
                {
                    "customer": "Customer A",
                    "quote": "SO-1",
                    "status": "Pending Fulfillment",
                    "date": "2026-05-01",
                    "inside_sales": "Sales Rep",
                    "item": "ITEM-1",
                    "item_description": "Item 1",
                    "brand": "Brand A",
                    "product_group": "Group A",
                    "selected_vendor": "Vendor A",
                    "qty": 10,
                    "unit_price": 100,
                    "unit_cost": 70,
                    "gross_margin_pct": 0.30,
                    "inbound_freight_cost": 0,
                    "outbound_freight_cost": 0,
                }
            ]
        )

        summary = summarize_sold_items(df)

        self.assertEqual(summary["overview"]["total_gross_margin"], 300)
        self.assertEqual(summary["overview"]["average_gross_margin_pct"], 0.30)


if __name__ == "__main__":
    unittest.main()

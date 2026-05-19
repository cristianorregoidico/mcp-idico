import unittest
from decimal import Decimal
from unittest.mock import mock_open, patch

from utils.json_df import save_result_to_json


class TestJsonDf(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open)
    def test_save_result_to_json_serializes_decimal_values(self, _mock_open):
        dataset = save_result_to_json(
            columns=["amount_usd", "created_at"],
            rows=[(Decimal("10.50"), "2026-05-12")],
            description="Dataset with decimal",
            name="decimal_dataset",
        )

        self.assertIn("filename", dataset)
        self.assertTrue(dataset["filename"].endswith("_decimal_dataset.json"))


if __name__ == "__main__":
    unittest.main()

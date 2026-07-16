import unittest

from features.sales import tools as sales_tools


class TestSalesToolsQuotes(unittest.TestCase):
    def test_get_quotes_rejects_invalid_approval_state(self):
        with self.assertRaisesRegex(ValueError, "Invalid approval_state 'Pending'"):
            sales_tools.get_quotes(approval_state="Pending")


if __name__ == "__main__":
    unittest.main()

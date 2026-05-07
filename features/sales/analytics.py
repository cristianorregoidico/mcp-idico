"""Compatibility facade for legacy sales analytics imports.

Business logic has been moved to capability-oriented domain modules.
"""

from features.sales.domain.bookings import finance_summary
from features.sales.domain.items import (
    quoted_brands_recurrence_metrics,
    sold_brands_recurrence_metrics,
    summarize_items_quoted,
    summarize_sold_items,
)
from features.sales.domain.opportunities import opportunity_summary
from features.sales.domain.quotes import (
    general_summary_is_q_so,
    summarize_is_quotes,
)
from features.sales.domain.vendor_recommendation import analize_hr_desviado

__all__ = [
    "finance_summary",
    "opportunity_summary",
    "sold_brands_recurrence_metrics",
    "quoted_brands_recurrence_metrics",
    "summarize_sold_items",
    "summarize_is_quotes",
    "summarize_items_quoted",
    "analize_hr_desviado",
    "general_summary_is_q_so",
]

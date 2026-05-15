from typing import Any, Dict, List, Optional

from features.financial.use_cases import aging
from utils.date import get_month_start_and_today


def get_aging(
    topic: str,
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    entity_name: Optional[str] = "",
    subsidiary: Optional[str] = "",
) -> Dict[str, Any]:
    """Retrieve accounts receivable or accounts payable aging KPIs for the selected period.

    Use this tool when the user asks for accounts receivable aging,
    accounts payable aging, cartera por cobrar, cuentas por pagar,
    customer or vendor overdue documents, collections priority,
    payment priorities, open AR or AP balances, or unapplied payments
    and credits.

    Args:
        topic: "receivable" for AR (customers) or "payable" for AP (vendors).
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        entity_name: Customer or vendor name substring to filter; optional.
        subsidiary: NetSuite subsidiary id ("3" Colombia, "4" Peru, "5" USA); optional.

    Returns:
        Dict[str, Any]: Aging buckets, totals by currency and subsidiary,
        top entities by open balance, concentration, housekeeping block
        with unapplied payments and credits, and dataset reference.
    """
    normalized_topic = (topic or "").lower().strip()
    if normalized_topic not in ("receivable", "payable"):
        raise ValueError(
            f"Invalid topic '{topic}'. Use 'receivable' (AR) or 'payable' (AP)."
        )

    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_entity_name = entity_name.upper() if entity_name else ""
    normalized_subsidiary = subsidiary.strip() if subsidiary else ""

    if normalized_topic == "receivable":
        return aging.execute_receivable_aging(
            start_q_date, final_q_date, normalized_entity_name, normalized_subsidiary
        )
    return aging.execute_payable_aging(
        start_q_date, final_q_date, normalized_entity_name, normalized_subsidiary
    )


FINANCIAL_TOOLS: List = [
    get_aging,
]

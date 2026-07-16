from typing import Any, Dict, List, Optional

from features.sales.use_cases import activity, bookings, items, opportunities, quotes, vendor_recommendation
from utils.date import get_month_start_and_today


ALLOWED_APPROVAL_STATES = {
    "unapproved": "Unapproved",
    "not required": "Not required",
    "approved": "Approved",
}


def get_quotes(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    inside_sales: Optional[str] = None,
    customer_name: Optional[str] = "",
    approval_state: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve summarized KPIs for quotes for the provided period.

    Use this tool when the user asks for quotes, quotes by customer,
    or quotes by Inside Sales.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        inside_sales: Inside Sales rep to filter; optional.
        customer_name: Customer name to filter; optional.
        approval_state: Approval status filter; optional. Allowed values only:
            Unapproved, Not required, Approved.

    Returns:
        Dict[str, Any]: KPIs per Inside Sales, status mix, win rate,
        commercial totals, period summaries and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_inside_sales = "" if not inside_sales else inside_sales.upper()
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_approval_state = ""
    if approval_state:
        approval_state_key = approval_state.strip().lower()
        normalized_approval_state = ALLOWED_APPROVAL_STATES.get(approval_state_key, "")
        if not normalized_approval_state:
            raise ValueError(
                "Invalid approval_state "
                f"'{approval_state}'. Allowed values: {list(ALLOWED_APPROVAL_STATES.values())}"
            )
    return quotes.execute(
        start_q_date,
        final_q_date,
        normalized_inside_sales,
        normalized_customer_name,
        normalized_approval_state,
    )


def get_bookings(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    customer_name: Optional[str] = "",
    inside_sales: Optional[str] = "",
    status: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve summarized KPIs for bookings for the provided period.

    Use this tool when the user asks for bookings, sales totals,
    bookings by customer, or bookings by Inside Sales.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.
        status: Booking status filter; optional. Must be one of the
            allowed Sales Order statuses handled by the use case
            ("Billed", "Pending Billing", "Pending Billing/Partially Fulfilled",
            "Partially Fulfilled", "Pending Fulfillment").

    Returns:
        Dict[str, Any]: Booking totals, margins, top customers,
        distributions, KPI summaries and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    normalized_status = status.strip() if status else None
    return bookings.execute(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales, normalized_status)


def get_quoted_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    """Retrieve summarized KPIs for quoted items for the provided period.

    Use this tool when the user asks for quoted items, quoted brands,
    quoted products by customer, or quoted products by Inside Sales.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.
        topic: Either "items" or "brand" to change the summary focus.

    Returns:
        Dict[str, Any]: Item or brand recurrence summaries, vendor and
        customer breakdowns, and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    return items.execute_quoted_items(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales, topic or "items")


def get_sold_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    """Retrieve summarized KPIs for sold items for the provided period.

    Use this tool when the user asks for sold items, sold brands,
    product mix, or sold products by customer or Inside Sales.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.
        topic: Either "items" or "brand" to change the summary focus.

    Returns:
        Dict[str, Any]: KPI summaries, top sold items, brand distributions,
        margin-related insights, and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    return items.execute_sold_items(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales, topic or "items")


def get_opportunities(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = "", customer_name: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for opportunities for the provided period.

    Use this tool when the user asks for opportunities, pipeline activity,
    commercial follow-up, or opportunities by customer or Inside Sales.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        inside_sales: Inside Sales filter; optional.
        customer_name: Customer filter; optional.

    Returns:
        Dict[str, Any]: Opportunity totals, customer participation,
        status distributions, overdue items and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_inside_sales = "" if not inside_sales else inside_sales.upper()
    normalized_customer_name = "" if not customer_name else customer_name.upper()
    return opportunities.execute(start_q_date, final_q_date, normalized_inside_sales, normalized_customer_name)


def get_vendors_to_quote(customer_name: str, brand: str) -> Dict[str, Any]:
    """Suggest vendors to quote for a specific customer and brand.

    Use this tool when the user wants vendor suggestions for a customer-brand
    combination based on historical customer-brand and country-brand behavior.

    Args:
        customer_name: Customer name; required.
        brand: Brand name; required.

    Returns:
        Dict[str, Any]: Suggested vendor lists and supporting metadata for
        customer-brand and country-brand matches.
    """
    if not customer_name or not brand:
        raise ValueError("Both customer_name and brand must be provided.")
    normalized_customer_name = customer_name.upper()
    normalized_brand = brand.upper()
    return vendor_recommendation.execute(normalized_customer_name, normalized_brand)


def get_events_summary(start_date: Optional[str], final_date: Optional[str], customer_name: Optional[str], organizer: Optional[str], subject: Optional[str]) -> Dict[str, Any]:
    """Retrieve and summarize commercial events or call activity.

    Use this tool when the user asks for event summaries, commercial calls,
    customer conversation context, or relationship insights.

    Args:
        start_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        customer_name: Customer filter; optional.
        organizer: Organizer filter; optional.
        subject: Subject filter; optional.

    Returns:
        Dict[str, Any]: Total activity count plus detailed records and a
        suggested prompt block for downstream qualitative analysis.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = start_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_organizer = organizer.upper() if organizer else ""
    normalized_subject = subject.upper() if subject else ""
    return activity.execute(start_q_date, final_q_date, normalized_customer_name, normalized_organizer, normalized_subject)


SALES_TOOLS: List = [
    get_quotes,
    get_bookings,
    get_quoted_items,
    get_sold_items,
    get_opportunities,
    get_vendors_to_quote,
    get_events_summary,
]

from typing import Any, Dict, List, Optional

from features.sales.use_cases import activity, bookings, items, opportunities, quotes, vendor_recommendation
from utils.date import get_month_start_and_today


def get_quotes(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None, customer_name: Optional[str] = "") -> Dict[str, Any]:
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_inside_sales = "" if not inside_sales else inside_sales.upper()
    normalized_customer_name = customer_name.upper() if customer_name else ""
    return quotes.execute(start_q_date, final_q_date, normalized_inside_sales, normalized_customer_name)


def get_bookings(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    return bookings.execute(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales)


def get_quoted_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    return items.execute_quoted_items(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales, topic or "items")


def get_sold_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_customer_name = customer_name.upper() if customer_name else ""
    normalized_inside_sales = inside_sales.upper() if inside_sales else ""
    return items.execute_sold_items(start_q_date, final_q_date, normalized_customer_name, normalized_inside_sales, topic or "items")


def get_opportunities(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = "", customer_name: Optional[str] = "") -> Dict[str, Any]:
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_inside_sales = "" if not inside_sales else inside_sales.upper()
    normalized_customer_name = "" if not customer_name else customer_name.upper()
    return opportunities.execute(start_q_date, final_q_date, normalized_inside_sales, normalized_customer_name)


def get_vendors_to_quote(customer_name: str, brand: str) -> Dict[str, Any]:
    if not customer_name or not brand:
        raise ValueError("Both customer_name and brand must be provided.")
    normalized_customer_name = customer_name.upper()
    normalized_brand = brand.upper()
    return vendor_recommendation.execute(normalized_customer_name, normalized_brand)


def get_events_summary(start_date: Optional[str], final_date: Optional[str], customer_name: Optional[str], organizer: Optional[str], subject: Optional[str]) -> Dict[str, Any]:
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

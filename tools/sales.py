from typing import Dict, List, Optional, Any
from utils.date import get_month_start_and_today
from utils.json_df import save_result_to_json, save_df_to_excel
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_bookings_data, get_items_quoted_by_customer, get_opportunities_data, get_sold_items_by_period
from analitycs.data_transformations import tuple_to_dataframe
from analitycs.sales import finance_summary, opportunity_summary, summarize_sold_items, summarize_is_quotes, summarize_items_quoted


def get_quotes(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None, customer_name: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for quotes for the provided period.
    
    Use this tool when user asks for quotes, quotes by customer or quotes by Inside Sales.

    Args:
        initial_date: Start date; defaults to today.
        final_date: End date; defaults to today.
        inside_sales: Inside Sales rep to filter; optional.
        customer_name: Customer name to filter; optional.

    Returns:
        Dict[str, Any]: KPIs per Inside Sales, status mix, win rate, incoterms, totals, period/timeline summaries plus dataset reference.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date
    
    inside_sales = "" if not inside_sales else inside_sales.upper()
    customer_name = customer_name.upper() if customer_name else ""

    sql = get_quotes_by_inside(f"{start_q_date}", f"{final_q_date}", inside_sales, customer_name)
    print("sql", sql)
    # Use the connection as a context manager for safe cleanup
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"Quotes by Inside Sales dataset between {initial_date} and {final_date}", name="get_quotes")

    df = tuple_to_dataframe(columns, rows)
    excel_file = save_df_to_excel(df, name="get_quotes")
    results = summarize_is_quotes(df)
    results["full_data_reference"] = dataset_reference
    results["excel_file"] = excel_file

    return results


def get_bookings(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for bookings for the provided period, inside sales or customer.
    
    Use this tool when user asks for bookings, bookings by customer or bookings by Inside Sales.

    Args:
        initial_date: Start date; defaults to first day of month.
        final_date: End date; defaults to today.
        customer_name: Optional customer filter; case-insensitive.
        inside_sales: Optional Inside Sales filter; case-insensitive.

    Returns:
        Dict[str, Any]: Totals, KPI by period/subsidiary, top customers, margin buckets plus dataset reference.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if customer_name:
        customer_name = customer_name.upper()
    if inside_sales:
        inside_sales = inside_sales.upper()

    sql = get_bookings_data(start_q_date, final_q_date, customer_name, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"Bookings dataset between {initial_date} and {final_date}", name="bookings_data")

    df = tuple_to_dataframe(columns, rows)
    summary = finance_summary(df)
    summary["full_data_reference"] = dataset_reference

    return summary

def get_quoted_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for quoted items for the provided period.
    
    Use this tool when user asks for quoted items, quoted items by customer or quoted items by Inside Sales.

    Args:
        initial_date: Start date; defaults to month start.
        final_date: End date; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.

    Returns:
        Dict[str, Any]: Vendor/brand ranking, Inside Sales coverage, customer–brand breakdown, top quoted items plus dataset reference.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    
    if customer_name:
        customer_name = customer_name.upper()
        
    if inside_sales:
        inside_sales = inside_sales.upper()
    sql = get_items_quoted_by_customer(start_q_date, final_q_date, customer_name, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"List of quoted items dataset between {initial_date} and {final_date}", name="quoted_items")
    df = tuple_to_dataframe(columns, rows)
    results = summarize_items_quoted(df)
    results["full_data_reference"] = dataset_reference

    return results

def get_sold_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for sold items for the provided period.
    
    Use this tool when user asks for sold items, sold items by customer or sold items by Inside Sales.

    Args:
        initial_date: Start date; defaults to month start.
        final_date: End date; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.

    Returns:
        Dict[str, Any]: KPIs, top items, vendor/brand breakdown, product group distribution plus dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date

    if customer_name:
        customer_name = customer_name.upper()
    if inside_sales:
        inside_sales = inside_sales.upper()

    sql = get_sold_items_by_period(start_q_date, final_q_date, customer_name, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"Sold items dataset between {initial_date} and {final_date}", name="sold_items_by_period")
    df = tuple_to_dataframe(columns, rows)
    summary = summarize_sold_items(df)
    summary["full_data_reference"] = dataset_reference

    return summary

def get_opportunities(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for opportunities for the provided period and Inside Sales.
    
    Use this tool when user asks for opportunities or opportunities by Inside Sales.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        inside_sales: Name or identifier of the Inside Sales rep to filter by (case-insensitive).

    Returns:
        Dict[str, Any]: KPIs per Inside Sales (total amount, order count, avg ticket), status breakdown with sales order lists, top customers per Inside Sales, and general totals plus period and timeline summaries.
    """
    
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date

    inside_sales = "" if not inside_sales else inside_sales.upper()
    
    sql = get_opportunities_data(start_q_date, final_q_date, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"Opportunities by Inside Sales dataset between {initial_date} and {final_date}", name="opportunity_by_is")

    df = tuple_to_dataframe(columns, rows)
    results = opportunity_summary(df)
    results["full_data_reference"] = dataset_reference

    return results



SALES_TOOLS: List = [
    get_quotes,
    get_bookings,
    get_quoted_items,
    get_sold_items,
    get_opportunities
]
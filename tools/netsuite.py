import os
from typing import Dict, List, Optional, Any
from utils.date import get_month_start_and_today
from utils.json_df import load_dataset_from_json, save_result_to_json
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_data, get_items_quoted_by_customer, get_opportunities_data, get_sold_items_by_period, get_op_so_data
from analitycs.data_transformations import tuple_to_dataframe, summarize_is_bookings, summarize_is_quotes, summarize_items_quoted
from analitycs.sales import finance_summary, opportunity_summary, analyze_inside_sales, summarize_sold_items

def quotes_overview(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Summarize quotes for a date range showing IS, customer, amount, margin, incoterms, etc.

    Args:
        initial_date: Start date; defaults to today.
        final_date: End date; defaults to today.
        inside_sales: Inside Sales rep to filter; optional.

    Returns:
        Dict[str, Any]: KPIs per Inside Sales, status mix, win rate, incoterms, totals, period/timeline summaries plus dataset reference.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date
    
    inside_sales = "" if not inside_sales else inside_sales.upper()

    sql = get_quotes_by_inside(f"{start_q_date}", f"{final_q_date}", inside_sales)
    print("sql", sql)
    # Use the connection as a context manager for safe cleanup
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"Quotes by Inside Sales dataset between {initial_date} and {final_date}", name="quotes_by_is")

    df = tuple_to_dataframe(columns, rows)
    results = summarize_is_quotes(df)
    results["full_data_reference"] = dataset_reference

    return results

def salesorders_overview(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Summarize sales orders for a date range showing IS, customer, amount, margin, incoterms, etc.

    Args:
        initial_date: Start date; defaults to today.
        final_date: End date; defaults to today.
        inside_sales: Inside Sales rep to filter; optional.

    Returns:
        Dict[str, Any]: KPIs per Inside Sales, status mix, top customers, totals, period/timeline summaries plus dataset reference.
    """
    
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date

    inside_sales = "" if not inside_sales else inside_sales.upper()
    
    sql = get_sales_orders_by_inside(start_q_date, final_q_date, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"Sales Orders by Inside Sales dataset between {initial_date} and {final_date}", name="sales_by_is")

    df = tuple_to_dataframe(columns, rows)
    results = summarize_is_bookings(df)
    results["full_data_reference"] = dataset_reference

    return results

def bookings_ovreview(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "") -> Dict[str, Any]:
    """Summarize bookings for a provided range showing gross margin by period, subsidiary, and customer.

    Args:
        initial_date: Start date; defaults to first day of month.
        final_date: End date; defaults to today.
        customer_name: Optional customer filter; case-insensitive.

    Returns:
        Dict[str, Any]: Totals, KPI by period/subsidiary, top customers, margin buckets plus dataset reference.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if customer_name:
        customer_name = customer_name.upper()

    sql = get_bookings_data(start_q_date, final_q_date, customer_name)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"Bookings dataset between {initial_date} and {final_date}", name="bookings_data")

    df = tuple_to_dataframe(columns, rows)
    summary = finance_summary(df)
    summary["full_data_reference"] = dataset_reference

    return summary

def quoted_items_overview(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """List items quoted by customer or Inside Sales for a date range.

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

def sold_items_overview(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """List sold items with margin/vendor breakdown for a date range.

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

def opportunity_insights(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Return summarized KPIs for opportunities for the provided period.

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

def inside_sales_performance(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> Dict[str, Any]:
    """Analyze Inside Sales performance for the selected period (Response time, hitrate).
    If no dates are provided, defaults to today's date.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.

    Returns:
        Dict[str, Any]: Inside Sales performance (Response time, hitrate, scorecard).
    """
    
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date

    
    sql = get_op_so_data(start_q_date, final_q_date)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"Inside Sales Performance dataset between {initial_date} and {final_date}", name="op_to_so")

    df = tuple_to_dataframe(columns, rows)
    results = analyze_inside_sales(df)
    results["full_data_reference"] = dataset_reference

    return results

NETSUITE_TOOLS: List = [
    quotes_overview,
    salesorders_overview,
    bookings_ovreview,
    quoted_items_overview,
    sold_items_overview,
    opportunity_insights,
    inside_sales_performance
]
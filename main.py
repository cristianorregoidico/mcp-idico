import os
from typing import Dict, List, Optional, Any
from fastmcp import FastMCP
from utils.date import get_month_start_and_today
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer, get_opportunities_by_inside, get_bookings_data
from analitycs.data_transformations import tuple_to_dataframe, summarize_bookings_data, summarize_customer_bookings, summarize_is_bookings, summarize_is_quotes, summarize_items_quoted
from analitycs.sales import finance_summary

app = FastMCP("idico-sales")


@app.tool
def quotes_by_inside_sales(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Fetch summarized Inside Sales quotes for a given date range. 
    If no Inside Sales rep is specified, all quotes are included.
    If no dates are provided, defaults to today's date.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        inside_sales: Name of the Inside Sales rep to filter by (case-insensitive).

    Returns:
        Dict[str, Any]: KPIs per Inside Sales (total amount, quote count, avg quote), status breakdown with quote lists, win-rate (by count and amount), incoterms mix, general totals plus period and timeline summaries.
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

    df = tuple_to_dataframe(columns, rows)
    results = summarize_is_quotes(df)

    return results

@app.tool
def sales_by_inside_sales(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Return summarized Inside Sales bookings (sales orders) for the selected period.
    If no Inside Sales rep is specified, all orders are included.
    If no dates are provided, defaults to today's date.

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
    
    sql = get_sales_orders_by_inside(f"{start_q_date}", f"{final_q_date}", inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    df = tuple_to_dataframe(columns, rows)
    results = summarize_is_bookings(df)

    return results

@app.tool
def bookings_insights(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "") -> Dict[str, Any]:
    """Summarize bookings and gross margin by period, subsidiary, and entity for a date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to the first day of the current month.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        customer_name: Optional Customer name to filter by (case-insensitive).

    Returns:
        Dict[str, Any]: General totals, KPI by period and subsidiary, top customers by gross amount, and margin bucket summary by customer. (Also note that the underlying query filters to specific subsidiaries and excludes certain entities/employees.)
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if customer_name:
        customer_name = customer_name.upper()

    sql = get_bookings_data({start_q_date}, {final_q_date}, customer_name)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    df = tuple_to_dataframe(columns, rows)
    summary = finance_summary(df)

    return summary

@app.tool
def items_quoted_by_customer(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Summarize items quoted to a specific customer for the selected date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to the first day of the current month.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        customer_name: Optional Customer name to filter by (case-insensitive).
        inside_sales: Optional Inside Sales representative filter (case-insensitive).

    Returns:
        Dict[str, Any]: Vendor ranking, brand ranking, inside sales coverage (brands/vendors/product groups), customer–brand breakdown, and top quoted items (lines, qty, value).
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    
    if customer_name:
        customer_name = customer_name.upper()
        
    if inside_sales:
        inside_sales = inside_sales.upper()
    sql = get_items_quoted_by_customer(f"{start_q_date}", f"{final_q_date}", customer_name, inside_sales)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    df = tuple_to_dataframe(columns, rows)
    results = summarize_items_quoted(df)
    print("Returning results")

    return results



if __name__ == "__main__":
    import asyncio
    try:
        app.run(
            transport="streamable-http",
            host="0.0.0.0",
            port=8000,
            path="/mcp",  # endpoint HTTP del servidor MCP
        )
    except asyncio.CancelledError:
        pass

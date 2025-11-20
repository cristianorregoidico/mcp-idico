from typing import Dict, List, Optional, Any
from mcp.server.fastmcp import FastMCP
from utils.date import get_month_start_and_today
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer, get_opportunities_by_inside, get_bookings_by_customer
from analitycs.data_transformations import tuple_to_dataframe, summarize_bookings_data, summarize_customer_bookings, summarize_is_bookings, summarize_is_quotes, summarize_items_quoted

app = FastMCP("idico-sales")


@app.tool()
def list_quotes(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Fetch summarized Inside Sales quotes for a given date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        inside_sales: Name or identifier of the Inside Sales rep to filter by (case-insensitive).

    Returns:
        Dict[str, Any]: Aggregated metrics from ``summarize_is_quotes`` keyed by column name.
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

@app.tool()
def list_sales_orders(initial_date: Optional[str] = None, final_date: Optional[str] = None, inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Return summarized Inside Sales bookings (sales orders) for the selected period.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        inside_sales: Name or identifier of the Inside Sales rep to filter by (case-insensitive).

    Returns:
        Dict[str, Any]: Aggregated booking metrics produced by ``summarize_is_bookings``.
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

@app.tool()
def bookings_by_period(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> Dict[str, Any]:
    """Summarize bookings and gross margin by period, subsidiary, and entity for a date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to the first day of the current month.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.

    Returns:
        Dict[str, Any]: Metrics grouped by period as returned by ``summarize_bookings_data``.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date


    sql = get_bookings_by_period(f"'{start_q_date}'", f"'{final_q_date}'")
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    df = tuple_to_dataframe(columns, rows)
    summary = summarize_bookings_data(df)

    return summary

@app.tool()
def bookings_by_customer_and_period(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = None) -> Dict[str, Any]:
    """Summarize bookings per period for a specific customer within a date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to the first day of the current month.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        customer_name: Customer name to filter by (required, case-insensitive).

    Returns:
        Dict[str, Any]: Aggregated metrics per period from ``summarize_customer_bookings`` or an error message if the customer is missing.
    """

     # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if not customer_name:
        return {"error": "customer_name is required"}
    sql = get_bookings_by_customer(f"{start_q_date}", f"{final_q_date}", customer_name.upper())
    print("sql", sql)

    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    df = tuple_to_dataframe(columns, rows)
    summary = summarize_customer_bookings(df)

    return summary

@app.tool()
def items_quoted_by_customer_and_period(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Summarize items quoted to a specific customer for the selected date range.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to the first day of the current month.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.
        customer_name: Customer name to filter by (required, case-insensitive).
        inside_sales: Optional Inside Sales representative filter (case-insensitive).

    Returns:
        Dict[str, Any]: Aggregated metrics returned by ``summarize_items_quoted``.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
        
    customer_name = customer_name.upper() 
    sql = get_items_quoted_by_customer(f"{start_q_date}", f"{final_q_date}", customer_name)
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
        app.run(transport="sse")
    except asyncio.CancelledError:
        pass

from typing import Dict, List, Optional, Any
from fastmcp import FastMCP
from utils.date import get_month_start_and_today
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer, get_opportunities_by_inside

app = FastMCP("idico-sales")


@app.tool
def list_quotes(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return quotes created by Inside Sales between initial_date and final_date.
    This tool can be used to get the daily quotes created by Inside Sales.

    If dates are not provided, defaults to today's date.
    The function uses NetSuiteConnection.managed() context manager and
    NetSuiteConnection.execute_query() to fetch results and return a list
    of dictionaries where keys are column names.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date

    sql = get_quotes_by_inside(f"{start_q_date}", f"{final_q_date}")
    print("sql", sql)
    # Use the connection as a context manager for safe cleanup
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    # Map rows (tuples) to dicts using column names
    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            # If no column names provided, return tuple under 'row'
            results.append({"row": row})

    return results

@app.tool
def list_sales_orders(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return sales orders by Inside Sales between initial_date and final_date if provided.
    This tool can be used to get the daily sales orders created by Inside Sales.
    
    If dates are not provided, defaults to today's date.
    The function uses NetSuiteConnection.managed() context manager and
    NetSuiteConnection.execute_query() to fetch results and return a list
    of dictionaries where keys are column names.
    """
    
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date
    
    sql = get_sales_orders_by_inside(f"{start_q_date}", f"{final_q_date}")
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            results.append({"row": row})

    return results

@app.tool
def bookings_and_gm_by_period(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return bookings and gross margin by period between initial_date and final_date.
    customer_name can be provided to filter by specific customer, the results are grouped by periodo, subsidiary and entity.

    If dates are not provided, defaults to current period.
    The function uses NetSuiteConnection.managed() context manager and
    NetSuiteConnection.execute_query() to fetch results and return a list
    of dictionaries where keys are column names.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if not customer_name:
        customer_name = 'NULL'

    sql = get_bookings_by_period(f"'{start_q_date}'", f"'{final_q_date}'", customer_name)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            results.append({"row": row})

    return results

@app.tool
def items_quoted_by_customer_and_period(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Returns quoted items details by period between initial_date and final_date and customer provided.
    customer_name is required to filter.

    If dates are not provided, defaults to current period.
    The function uses NetSuiteConnection.managed() context manager and
    NetSuiteConnection.execute_query() to fetch results and return a list
    of dictionaries where keys are column names.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if not customer_name:
        return {"error": "customer_name is required"}
        
    customer_name = customer_name.upper() 
    sql = get_items_quoted_by_customer(f"{start_q_date}", f"{final_q_date}", customer_name)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            results.append({"row": row})

    return results

@app.tool
def opportunities_by_inside_sales(initial_date: Optional[str] = None, final_date: Optional[str] = None, sales_rep: Optional[str] = None) -> List[Dict[str, Any]]:
    """Returns the opportunities created by inside sales in a provided date range.

    If dates are not provided, defaults to current period.
    The function uses NetSuiteConnection.managed() context manager and
    NetSuiteConnection.execute_query() to fetch results and return a list
    of dictionaries where keys are column names.
    """
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    if not sales_rep:
        return {"error": "customer_name is required"}
        
    sales_rep = sales_rep.upper() 
    sql = get_opportunities_by_inside(f"{start_q_date}", f"{final_q_date}", sales_rep)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            results.append({"row": row})

    return results


if __name__ == "__main__":
    app.run(transport="sse", host="0.0.0.0", port=8000)

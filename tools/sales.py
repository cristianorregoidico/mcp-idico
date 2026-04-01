from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from utils.date import get_month_start_and_today
from utils.json_df import save_result_to_json, save_df_to_excel
from utils.envelope import build_tool_response
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_bookings_data, get_items_quoted_by_customer, get_opportunities_data, get_sold_items_by_period
from analitycs.data_transformations import tuple_to_dataframe
from analitycs.sales import (
    finance_summary,
    opportunity_summary,
    sold_brands_recurrence_metrics,
    quoted_brands_recurrence_metrics,
    summarize_sold_items,
    summarize_is_quotes,
    summarize_items_quoted,
    analize_hr_desviado
)
from connections.postgresql_querys import get_calls_summary, get_vendors_customer_brand, get_customer_country, get_vendors_country_brand
from connections.postgresql import execute_pg_query_dev


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
    start_q_date = initial_date or start_of_month
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
    # excel_file = save_df_to_excel(df, name="get_quotes")
    results = summarize_is_quotes(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_quotes",
        summary=results,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
            "inside_sales": inside_sales or None,
            "customer_name": customer_name or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
        # excel_file=excel_file,
    )


def get_bookings(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "") -> Dict[str, Any]:
    """Retrieve summarized KPIs for bookings for the provided period, inside sales or customer.
    
    Use this tool when user asks for bookings, bookings by customer, bookings by Inside Sales or sales in general.

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
    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_bookings",
        summary=summary,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
            "customer_name": customer_name or None,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

def get_quoted_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    """Retrieve summarized KPIs for quoted items for the provided period.
    
    Use this tool when user asks for quoted items, quoted items by customer or quoted items by Inside Sales.

    Args:
        initial_date: Start date; defaults to month start.
        final_date: End date; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.
        topic: Topic for the summary; should be "items" or "brand" defaults to "items".

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
    if topic == "brand":
        results = quoted_brands_recurrence_metrics(df)
    else:   
        results = summarize_items_quoted(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_quoted_items",
        summary=results,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
            "customer_name": customer_name or None,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

def get_sold_items(initial_date: Optional[str] = None, final_date: Optional[str] = None, customer_name: Optional[str] = "", inside_sales: Optional[str] = "", topic: Optional[str] = "items") -> Dict[str, Any]:
    """Retrieve summarized KPIs for sold items for the provided period.
    
    Use this tool when user asks for sold items, sold items by customer or sold items by Inside Sales.

    Args:
        initial_date: Start date; defaults to month start.
        final_date: End date; defaults to today.
        customer_name: Customer filter; optional.
        inside_sales: Inside Sales filter; optional.
        topic: Topic for the summary; should be "items" or "brand" defaults to "items".

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
    
    if topic == "brand":
        summary = sold_brands_recurrence_metrics(df)
    else:
        summary = summarize_sold_items(df)
    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_sold_items",
        summary=summary,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
            "customer_name": customer_name or None,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

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
    start_q_date = initial_date or start_of_month
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
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_opportunities",
        summary=results,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

def get_vendors_to_quote(customer_name: str, brand: str) -> Dict[str, Any]:
    """Retrieve a list of vendors to quote for a given customer and brand.
    
    Use this tool when user asks for vendors to quote for a specific customer and brand.

    Args:
        customer_name: Name of the customer (case-insensitive).
        brand: Brand name to filter by (case-insensitive).
    Returns:
        Dict[str, Any]: List of vendors that should be quoted for the specified customer and brand, along with any relevant details to assist in the quoting process.
    """
    if not customer_name or not brand:
        raise ValueError("Both customer_name and brand must be provided.")
    customer_name = customer_name.upper()
    brand = brand.upper()
    
    # Define the SQL for data with customer and brand
    sql_cus_brand = get_vendors_customer_brand(customer_name, brand)
    print("sql", sql_cus_brand)
    columns_cus_brand, rows_cus_brand = execute_pg_query_dev(sql_cus_brand)
    df_cus_brand = tuple_to_dataframe(columns_cus_brand, rows_cus_brand)
    
    # Get the customer's country to further filter vendors by brand and country
    sql_country = get_customer_country(customer_name)
    print("sql_country", sql_country)
    columns_country, rows_country = execute_pg_query_dev(sql_country)
    country = rows_country[0][0] if rows_country else ''
    
    # Define the SQL for data with country and brand
    sql_country_brand = get_vendors_country_brand(country, brand)
    print("sql_country_brand", sql_country_brand)
    columns_country_brand, rows_country_brand = execute_pg_query_dev(sql_country_brand)
    df_country_brand = tuple_to_dataframe(columns_country_brand, rows_country_brand)

    summary = analize_hr_desviado(df_cus_brand, df_country_brand)

    return build_tool_response(
        tool_name="get_vendors_to_quote",
        summary=summary,
        filters={
            "customer_name": customer_name,
            "brand": brand,
            "customer_country": country or None,
        },
        source_systems=["postgresql"],
        columns=columns_cus_brand,
        rows=rows_cus_brand,
        details={
            "customer_brand_matches": {
                "row_count": len(rows_cus_brand),
                "columns": columns_cus_brand,
            },
            "country_brand_matches": {
                "row_count": len(rows_country_brand),
                "columns": columns_country_brand,
            },
        },
    )
    
def get_calls_events_insights(start_date: Optional[str], final_date: Optional[str], customer_name: Optional[str], organizer: Optional[str], subject: Optional[str]) -> Dict[str, Any]:
    """Retrieve insights about customer relationships for a given customer.
    
    Use this tool when user asks for calls summary, events insights, modjo or in general for requests about customer conversations.

    Args:
        start_date: The start date for filtering calls data.
        final_date: The final date for filtering calls data.
        customer_name: Name of the customer (case-insensitive).
        organizer: Name of the organizer (case-insensitive).
        subject: Subject of the relationship (case-insensitive).
    Returns:
        Dict[str, Any]: Insights about the customer's relationships, including key contacts, communication history, and any relevant notes or flags that could impact sales strategies.
    """
    
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = start_date or start_of_month
    final_q_date = final_date or today_date
    sql = get_calls_summary(start_q_date, final_q_date, customer_name, organizer, subject)
    print("sql", sql)
    columns, rows = execute_pg_query_dev(sql)
    df = tuple_to_dataframe(columns, rows)
    calls_summary = df.to_dict(orient="records")
    
    return build_tool_response(
        tool_name="get_relationships_insights",
        summary={"total_calls": len(rows)},
        filters={
            "start_date": start_q_date,
            "final_date": final_q_date,
            "customer_name": customer_name or '',
            "organizer": organizer or '',
            "subject": subject or '',
        },
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        details={
            "suggested_prompt_for_insights": f"""
            A continuación se presentan los resúmenes de llamadas de ventas de IDICO. Tu objetivo es actuar como un Analista de Marketing Estratégico. Analiza los textos y responde exclusivamente basado en esta información:
            1. Identifica las 3 objeciones más recurrentes.
            2. Enumera qué características o valores mencionan que aprecian de IDICO.
            3. Describe quiénes suelen estar presentes (roles) según el campo attendees y la descripción.
            4. Clasifica las menciones en categorías (Precio, Servicio, Tecnología, etc.)""",
            "calls_summary": calls_summary,
        },
    )

    



SALES_TOOLS: List = [
    get_quotes,
    get_bookings,
    get_quoted_items,
    get_sold_items,
    get_opportunities,
    get_vendors_to_quote,
    get_calls_events_insights
]

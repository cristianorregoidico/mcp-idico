from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.sales.queries.items import get_items_quoted_by_customer, get_sold_items_by_period
from features.sales.domain.items import (
    quoted_brands_recurrence_metrics,
    sold_brands_recurrence_metrics,
    summarize_items_quoted,
    summarize_sold_items,
)
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute_quoted_items(initial_date: str, final_date: str, customer_name: str, inside_sales: str, topic: str) -> Dict[str, Any]:
    sql, params = get_items_quoted_by_customer(initial_date, final_date, customer_name, inside_sales)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(columns, rows, f"List of quoted items dataset between {initial_date} and {final_date}", name="quoted_items")
    df = tuple_to_dataframe(columns, rows)
    results = quoted_brands_recurrence_metrics(df) if topic == "brand" else summarize_items_quoted(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_quoted_items",
        summary=results,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            "customer_name": customer_name or None,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )


def execute_sold_items(initial_date: str, final_date: str, customer_name: str, inside_sales: str, topic: str) -> Dict[str, Any]:
    sql, params = get_sold_items_by_period(initial_date, final_date, customer_name, inside_sales)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(columns, rows, f"Sold items dataset between {initial_date} and {final_date}", name="sold_items_by_period")
    df = tuple_to_dataframe(columns, rows)
    summary = sold_brands_recurrence_metrics(df) if topic == "brand" else summarize_sold_items(df)
    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_sold_items",
        summary=summary,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            "customer_name": customer_name or None,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

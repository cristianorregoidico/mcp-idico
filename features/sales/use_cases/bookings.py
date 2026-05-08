from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.sales.queries.bookings import get_bookings_data
from features.sales.domain.bookings import finance_summary
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> Dict[str, Any]:
    sql, params = get_bookings_data(initial_date, final_date, customer_name, inside_sales)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(columns, rows, f"Bookings dataset between {initial_date} and {final_date}", name="bookings_data")
    df = tuple_to_dataframe(columns, rows)
    summary = finance_summary(df)
    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_bookings",
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

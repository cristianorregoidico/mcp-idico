from typing import Any, Dict, Optional

from connections.netsuite.client import NetSuiteConnection
from connections.netsuite.queries import get_quotes_by_inside
from features.sales.domain.quotes import summarize_is_quotes
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(initial_date: str, final_date: str, inside_sales: str, customer_name: str) -> Dict[str, Any]:
    sql = get_quotes_by_inside(initial_date, final_date, inside_sales, customer_name)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(
        columns,
        rows,
        f"Quotes by Inside Sales dataset between {initial_date} and {final_date}",
        name="get_quotes",
    )

    df = tuple_to_dataframe(columns, rows)
    results = summarize_is_quotes(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_quotes",
        summary=results,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            "inside_sales": inside_sales or None,
            "customer_name": customer_name or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

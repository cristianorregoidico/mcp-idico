from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from connections.netsuite.queries import get_opportunities_data
from features.sales.domain.opportunities import opportunity_summary
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(initial_date: str, final_date: str, inside_sales: str, customer_name: str) -> Dict[str, Any]:
    sql = get_opportunities_data(initial_date, final_date, inside_sales, customer_name)
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
            "initial_date": initial_date,
            "final_date": final_date,
            "inside_sales": inside_sales or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

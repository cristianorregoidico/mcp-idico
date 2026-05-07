from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from connections.netsuite.queries import get_op_so_data
from features.performance.domain.inside_sales import analyze_inside_sales
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(initial_date: str, final_date: str) -> Dict[str, Any]:
    sql = get_op_so_data(initial_date, final_date)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)

    dataset_reference = save_result_to_json(columns, rows, f"Inside Sales Performance dataset between {initial_date} and {final_date}", name="op_to_so")
    df = tuple_to_dataframe(columns, rows)
    results = analyze_inside_sales(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_inside_sales_performance_report",
        summary=results,
        filters={"initial_date": initial_date, "final_date": final_date},
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

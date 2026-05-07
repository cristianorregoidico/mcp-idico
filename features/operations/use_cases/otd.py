from typing import Any, Dict, Optional

from connections.postgresql.client import execute_pg_query_dev
from connections.postgresql.queries import get_on_time_delivery
from features.operations.domain.otd import on_time_delivery_summary
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(initial_date: str, final_date: str, so_number: Optional[str]) -> Dict[str, Any]:
    sql = get_on_time_delivery(initial_date, final_date, so_number)
    columns, rows = execute_pg_query_dev(sql)
    dataset_reference = save_result_to_json(columns, rows, "The full items delivery by period", name="otd_data")
    df = tuple_to_dataframe(columns, rows)
    results = on_time_delivery_summary(df)
    if so_number:
        results["so_details"] = df.to_dict(orient="records")

    return build_tool_response(
        tool_name="get_otd_indicators",
        summary=results,
        filters={"initial_date": initial_date, "final_date": final_date, "so_number": so_number},
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

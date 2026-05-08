from typing import Any, Dict, Optional

from connections.postgresql.client import execute_pg_query
from features.operations.queries.guides import get_helga_guides_query
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute(po: Optional[str], status: Optional[str], service: Optional[str]) -> Dict[str, Any]:
    sql, params = get_helga_guides_query(po=po, status=status, service=service)
    columns, rows = execute_pg_query(sql, params)
    dataset_reference = save_result_to_json(columns, rows, "List of guides pending for delivery", name="guides_oneding_delivery")
    results = tuple_to_dataframe(columns, rows).to_dict(orient="records")

    return build_tool_response(
        tool_name="get_helga_guides",
        summary={"result_count": len(results)},
        filters={"po": po, "status": status, "service": service},
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
        details={"results": results},
    )

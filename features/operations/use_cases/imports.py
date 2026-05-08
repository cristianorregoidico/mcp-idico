from typing import Any, Dict

from connections.postgresql.client import execute_pg_query_dev
from features.operations.queries.imports import get_customer_imports_data
from features.operations.domain.imports import build_imports_summary
from utils.envelope import build_tool_response
from utils.transformations import tuple_to_dataframe


def execute(customer_name: str) -> Dict[str, Any]:
    sql, params = get_customer_imports_data(customer_name)
    columns, rows = execute_pg_query_dev(sql, params)
    df = tuple_to_dataframe(columns, rows)
    summary = build_imports_summary(df)
    return build_tool_response(
        tool_name="get_customer_imports",
        summary=summary,
        filters={"customer_name": customer_name},
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
    )

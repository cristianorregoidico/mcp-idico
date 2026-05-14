from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from connections.postgresql.client import execute_pg_query
from features.operations.domain.idra_usage import build_idra_usage_metrics
from features.operations.queries.idra_usage import get_idra_usage_query
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe

DATE_FORMAT = "%Y-%m-%d"


def _validate_date(date_value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(date_value, DATE_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} '{date_value}'. Expected YYYY-MM-DD") from exc


def _resolve_date_range(initial_date: str, final_date: str) -> tuple[str, str]:
    start_date = _validate_date(initial_date, "initial_date")
    end_date = _validate_date(final_date, "final_date")

    if start_date > end_date:
        raise ValueError("initial_date must be less than or equal to final_date")

    end_date_exclusive = end_date + timedelta(days=1)
    return initial_date, end_date_exclusive.strftime(DATE_FORMAT)


def execute(initial_date: str, final_date: str) -> Dict[str, Any]:
    resolved_initial_date, resolved_final_date_exclusive = _resolve_date_range(initial_date, final_date)
    sql, params = get_idra_usage_query(resolved_initial_date, resolved_final_date_exclusive)

    try:
        columns, rows = execute_pg_query(sql, params)
    except Exception as exc:
        raise RuntimeError("Failed to retrieve IDRA usage metrics from PostgreSQL") from exc

    dataset_reference = save_result_to_json(
        columns,
        rows,
        "IDRA usage raw dataset",
        name="idra_usage_data",
    )

    metrics = build_idra_usage_metrics(tuple_to_dataframe(columns, rows))

    return build_tool_response(
        tool_name="get_idra_usage",
        summary=metrics,
        filters={
            "initial_date": resolved_initial_date,
            "final_date": final_date,
        },
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
        details={
            "dataset_reference": dataset_reference["filename"],
            "row_count": len(rows),
            "notes": [
                "response is stored as text and classified heuristically",
                "unknown responses are kept separate from errors",
            ],
        },
    )

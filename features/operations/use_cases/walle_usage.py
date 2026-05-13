from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from connections.postgresql.client import execute_pg_query
from features.operations.domain.walle_usage import build_walle_usage_metrics
from features.operations.queries.walle_usage import (
    get_action_suggestions_query,
    get_analyzed_emails_query,
    get_summarized_emails_query,
    get_walle_event_log_query,
)
from utils.date import get_month_start_and_today
from utils.envelope import build_tool_response
from utils.json_df import save_dataset_manifest, save_result_to_json
from utils.transformations import tuple_to_dataframe

MAX_SAMPLE_LIMIT = 500
DATE_FORMAT = "%Y-%m-%d"


def _validate_date(date_value: str, field_name: str) -> None:
    try:
        datetime.strptime(date_value, DATE_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name} '{date_value}'. Expected YYYY-MM-DD") from exc


def _normalize_limit(limit: int) -> int:
    if limit <= 0:
        raise ValueError("limit must be greater than 0")
    return min(limit, MAX_SAMPLE_LIMIT)


def _normalize_po_name(po_name: Optional[str]) -> Optional[str]:
    normalized = (po_name or "").strip()
    return normalized or None


def _resolve_dates(initial_date: Optional[str], final_date: Optional[str], po_name: Optional[str]) -> tuple[str, str]:
    resolved_initial_date = initial_date
    resolved_final_date = final_date

    if not po_name and (not resolved_initial_date or not resolved_final_date):
        resolved_initial_date, resolved_final_date = get_month_start_and_today()

    if not resolved_initial_date or not resolved_final_date:
        resolved_initial_date, resolved_final_date = get_month_start_and_today()

    _validate_date(resolved_initial_date, "initial_date")
    _validate_date(resolved_final_date, "final_date")

    if resolved_initial_date > resolved_final_date:
        raise ValueError("initial_date must be less than or equal to final_date")

    return resolved_initial_date, resolved_final_date


def _save_raw_dataset(columns: list[str], rows: list[tuple[Any, ...]], description: str, name: str) -> dict[str, Any]:
    return save_result_to_json(columns, rows, description, name=name)


def execute(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    po_name: Optional[str] = None,
    limit: int = 100,
) -> Dict[str, Any]:
    normalized_po_name = _normalize_po_name(po_name)
    sample_limit = _normalize_limit(limit)
    resolved_initial_date, resolved_final_date = _resolve_dates(initial_date, final_date, normalized_po_name)

    query_specs = {
        "summarized_emails": get_summarized_emails_query(resolved_initial_date, resolved_final_date, normalized_po_name),
        "analyzed_emails": get_analyzed_emails_query(resolved_initial_date, resolved_final_date, normalized_po_name),
        "action_suggestions": get_action_suggestions_query(resolved_initial_date, resolved_final_date, normalized_po_name),
        "event_log": get_walle_event_log_query(resolved_initial_date, resolved_final_date),
    }

    results: dict[str, dict[str, Any]] = {}

    try:
        for dataset_key, (sql, params) in query_specs.items():
            columns, rows = execute_pg_query(sql, params)
            results[dataset_key] = {
                "columns": columns,
                "rows": rows,
                "dataframe": tuple_to_dataframe(columns, rows),
            }
    except Exception as exc:
        raise RuntimeError("Failed to retrieve Walle usage metrics from PostgreSQL") from exc

    dataset_artifacts = {
        "summarized_emails": _save_raw_dataset(
            results["summarized_emails"]["columns"],
            results["summarized_emails"]["rows"],
            "Walle summarized emails raw dataset",
            name="walle_summarized_emails_data",
        ),
        "analyzed_emails": _save_raw_dataset(
            results["analyzed_emails"]["columns"],
            results["analyzed_emails"]["rows"],
            "Walle analyzed emails raw dataset",
            name="walle_analyzed_emails_data",
        ),
        "action_suggestions": _save_raw_dataset(
            results["action_suggestions"]["columns"],
            results["action_suggestions"]["rows"],
            "Walle action suggestions raw dataset",
            name="walle_action_suggestions_data",
        ),
        "event_log": _save_raw_dataset(
            results["event_log"]["columns"],
            results["event_log"]["rows"],
            "Walle event log raw dataset",
            name="walle_event_log_data",
        ),
    }

    manifest_reference = save_dataset_manifest(
        {
            key: artifact["filename"]
            for key, artifact in dataset_artifacts.items()
        },
        description="Walle usage metrics raw datasets manifest",
        name="walle_usage_manifest",
    )

    summary = build_walle_usage_metrics(
        summarized_emails_df=results["summarized_emails"]["dataframe"],
        analyzed_emails_df=results["analyzed_emails"]["dataframe"],
        action_suggestions_df=results["action_suggestions"]["dataframe"],
        event_log_df=results["event_log"]["dataframe"],
    )

    details = {
        "note": "Detailed records are available through dataset_references. Request the dataset when row-level detail is needed.",
        "dataset_references": {
            key: artifact["filename"]
            for key, artifact in dataset_artifacts.items()
        },
        "row_counts": {
            key: int(len(result["rows"]))
            for key, result in results.items()
        },
        "sample_limit": sample_limit,
    }

    return build_tool_response(
        tool_name="get_walle_usage",
        summary=summary,
        filters={
            "initial_date": resolved_initial_date,
            "final_date": resolved_final_date,
            "po_name": normalized_po_name,
            "limit": sample_limit,
        },
        source_systems=["postgresql"],
        columns=[],
        rows=[],
        dataset_reference=manifest_reference,
        details=details,
    )

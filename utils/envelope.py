from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_time_range(filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start_date = filters.get("initial_date") or filters.get("start_date")
    end_date = filters.get("final_date") or filters.get("end_date")
    if not start_date and not end_date:
        return None
    return {
        "start_date": start_date,
        "end_date": end_date,
    }


def build_tool_response(
    *,
    tool_name: str,
    summary: Any,
    filters: Optional[Dict[str, Any]] = None,
    source_systems: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    rows: Optional[List[Any]] = None,
    dataset_reference: Optional[Dict[str, Any]] = None,
    # excel_file: Optional[str] = None,
    details: Optional[Any] = None,
) -> Dict[str, Any]:
    filters = filters or {}
    source_systems = source_systems or []

    meta: Dict[str, Any] = {
        "tool_name": tool_name,
        "generated_at": _iso_now(),
        "source_systems": source_systems,
        "filters": filters,
        "row_count": len(rows) if rows is not None else None,
        "column_count": len(columns) if columns is not None else None,
        "columns": columns or [],
        "time_range": _build_time_range(filters),
        "dataset_reference": dataset_reference["filename"] if dataset_reference else None,
        # "excel_file": excel_file,
    }

    response: Dict[str, Any] = {
        "meta": meta,
        "kpi_metrics": summary,
        "artifacts": {
            "dataset": dataset_reference,
            # "excel_file": excel_file,
        },
    }

    if details is not None:
        response["details"] = details

    return response

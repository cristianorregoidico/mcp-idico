from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.to_dict(orient="records")


def _series_distribution(series: pd.Series, key_name: str, value_name: str = "count") -> list[dict[str, Any]]:
    if series.empty:
        return []

    counts = series.fillna("Unknown").value_counts(dropna=False).reset_index()
    counts.columns = [key_name, value_name]
    return _to_records(counts)


def _safe_datetime_max(df: pd.DataFrame, column: str) -> str | None:
    if column not in df.columns or df.empty:
        return None

    values = pd.to_datetime(df[column], errors="coerce").dropna()
    if values.empty:
        return None
    return values.max().isoformat()


def _safe_numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns or df.empty:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").dropna()


def _empty_overview() -> dict[str, int]:
    return {
        "processed_po_count": 0,
        "summarized_email_count": 0,
        "analyzed_email_count": 0,
        "suggested_action_records": 0,
        "walle_event_count": 0,
    }


def build_walle_usage_metrics(
    summarized_emails_df: pd.DataFrame,
    analyzed_emails_df: pd.DataFrame,
    action_suggestions_df: pd.DataFrame,
    event_log_df: pd.DataFrame,
) -> Dict[str, Any]:
    po_series = pd.concat(
        [
            summarized_emails_df.get("po_name", pd.Series(dtype="object")),
            analyzed_emails_df.get("po_name", pd.Series(dtype="object")),
            action_suggestions_df.get("po_name", pd.Series(dtype="object")),
        ],
        ignore_index=True,
    )

    processed_po_count = int(po_series.dropna().nunique())
    summarized_email_count = (
        int(summarized_emails_df["email_id"].dropna().nunique()) if "email_id" in summarized_emails_df.columns else 0
    )
    analyzed_email_count = (
        int(analyzed_emails_df["email_id"].dropna().nunique()) if "email_id" in analyzed_emails_df.columns else 0
    )

    overview = _empty_overview()
    overview.update(
        {
            "processed_po_count": processed_po_count,
            "summarized_email_count": summarized_email_count,
            "analyzed_email_count": analyzed_email_count,
            "suggested_action_records": int(len(action_suggestions_df.index)),
            "walle_event_count": int(len(event_log_df.index)),
        }
    )

    summarized_latest_email_date = _safe_datetime_max(summarized_emails_df, "email_date")
    email_summary = {
        "emails_by_type": _series_distribution(
            summarized_emails_df.get("email_type", pd.Series(dtype="object")),
            "email_type",
        ),
        "vendors": _series_distribution(
            summarized_emails_df.get("vendor_name", pd.Series(dtype="object")),
            "vendor_name",
        ),
        "po_status_distribution": _series_distribution(
            summarized_emails_df.get("po_status", pd.Series(dtype="object")),
            "po_status",
        ),
        "latest_email_date": summarized_latest_email_date,
    }

    confidence_series = _safe_numeric_series(analyzed_emails_df, "confidence")
    email_analysis = {
        "avg_confidence": float(confidence_series.mean()) if not confidence_series.empty else 0.0,
        "scenario_distribution": _series_distribution(
            analyzed_emails_df.get("scenario", pd.Series(dtype="object")),
            "scenario",
        ),
        "version_distribution": _series_distribution(
            analyzed_emails_df.get("version", pd.Series(dtype="object")),
            "version",
        ),
    }

    actions_by_po = []
    latest_action_created_at = None
    latest_action_updated_at = None
    if not action_suggestions_df.empty and "po_name" in action_suggestions_df.columns:
        actions_by_po_df = (
            action_suggestions_df.groupby("po_name", dropna=False)
            .size()
            .reset_index(name="action_count")
            .sort_values(["action_count", "po_name"], ascending=[False, True])
        )
        actions_by_po = _to_records(actions_by_po_df)
        latest_action_created_at = _safe_datetime_max(action_suggestions_df, 'createdAt')
        latest_action_updated_at = _safe_datetime_max(action_suggestions_df, 'updatedAt')

    actions = {
        "actions_by_po": actions_by_po,
        "total_action_records": int(len(action_suggestions_df.index)),
        "latest_created_at": latest_action_created_at,
        "latest_updated_at": latest_action_updated_at,
    }

    duration_series = _safe_numeric_series(event_log_df, "duration_ms")
    status_series = _safe_numeric_series(event_log_df, "status_code")
    error_count = int((status_series >= 400).sum()) if not status_series.empty else 0
    total_events = int(len(event_log_df.index))

    events_by_endpoint = []
    if not event_log_df.empty and "endpoint" in event_log_df.columns:
        endpoint_usage = (
            event_log_df.groupby("endpoint", dropna=False)
            .agg(
                event_count=("endpoint", "size"),
                avg_duration_ms=("duration_ms", "mean"),
                max_duration_ms=("duration_ms", "max"),
            )
            .reset_index()
        )
        endpoint_usage["avg_duration_ms"] = pd.to_numeric(endpoint_usage["avg_duration_ms"], errors="coerce").fillna(0.0)
        endpoint_usage["max_duration_ms"] = pd.to_numeric(endpoint_usage["max_duration_ms"], errors="coerce").fillna(0.0)
        events_by_endpoint = _to_records(endpoint_usage)

    api_usage = {
        "events_by_endpoint": events_by_endpoint,
        "status_code_distribution": _series_distribution(
            event_log_df.get("status_code", pd.Series(dtype="object")),
            "status_code",
        ),
        "duration_ms": {
            "avg": float(duration_series.mean()) if not duration_series.empty else 0.0,
            "p50": float(duration_series.quantile(0.50)) if not duration_series.empty else 0.0,
            "p95": float(duration_series.quantile(0.95)) if not duration_series.empty else 0.0,
            "max": float(duration_series.max()) if not duration_series.empty else 0.0,
        },
        "error_count": error_count,
        "error_rate_pct": float((error_count / total_events) * 100.0) if total_events else 0.0,
    }

    return {
        "overview": overview,
        "email_summary": email_summary,
        "email_analysis": email_analysis,
        "actions": actions,
        "api_usage": api_usage,
    }

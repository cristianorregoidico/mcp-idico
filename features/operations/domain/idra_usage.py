from __future__ import annotations

import ast
import json
from typing import Any, Dict

import pandas as pd


WEEKDAY_ORDER = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.to_dict(orient="records")


def classify_response(response: Any) -> dict[str, Any]:
    text = "" if response is None else str(response).strip()

    if not text:
        return {
            "response_kind": "empty",
            "execution_status": "unknown",
            "error_message": None,
        }

    if text.startswith("ERROR:"):
        return {
            "response_kind": "error_text",
            "execution_status": "error",
            "error_message": text[6:].strip() or text,
        }

    try:
        parsed_json = json.loads(text)
        if isinstance(parsed_json, (dict, list)):
            return {
                "response_kind": "json_text",
                "execution_status": "success",
                "error_message": None,
            }
    except (json.JSONDecodeError, TypeError):
        pass

    try:
        parsed_literal = ast.literal_eval(text)
        if isinstance(parsed_literal, (dict, list)):
            return {
                "response_kind": "python_dict_text",
                "execution_status": "success",
                "error_message": None,
            }
    except (ValueError, SyntaxError):
        pass

    return {
        "response_kind": "unknown_text",
        "execution_status": "unknown",
        "error_message": None,
    }


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "tool_name",
                "username",
                "response",
                "duration_ms",
                "created_at",
                "date",
                "hour",
                "weekday",
                "response_kind",
                "execution_status",
                "error_message",
                "is_success",
                "is_error",
                "is_unknown",
            ]
        )

    prepared = df.copy()
    prepared["created_at"] = pd.to_datetime(prepared["created_at"], errors="coerce")
    prepared["duration_ms"] = pd.to_numeric(prepared.get("duration_ms"), errors="coerce")

    classifications = prepared.get("response", pd.Series(dtype="object")).apply(classify_response)
    classification_df = pd.DataFrame(classifications.tolist(), index=prepared.index)
    prepared = pd.concat([prepared, classification_df], axis=1)

    prepared["date"] = prepared["created_at"].dt.strftime("%Y-%m-%d")
    prepared["hour"] = prepared["created_at"].dt.hour
    prepared["weekday"] = pd.Categorical(
        prepared["created_at"].dt.day_name(),
        categories=WEEKDAY_ORDER,
        ordered=True,
    )
    prepared["is_success"] = prepared["execution_status"].eq("success")
    prepared["is_error"] = prepared["execution_status"].eq("error")
    prepared["is_unknown"] = prepared["execution_status"].eq("unknown")
    return prepared


def _distribution(df: pd.DataFrame, group_col: str, count_name: str) -> list[dict[str, Any]]:
    if df.empty or group_col not in df.columns:
        return []

    summary = (
        df.assign(**{group_col: df[group_col].fillna("Unknown")})
        .groupby(group_col, dropna=False)
        .size()
        .reset_index(name=count_name)
        .sort_values([count_name, group_col], ascending=[False, True])
    )
    return _to_records(summary)


def _top_tools(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    total_calls = len(df.index)
    summary = (
        df.groupby("tool_name", dropna=False)
        .agg(
            call_count=("tool_name", "size"),
            unique_users=("username", pd.Series.nunique),
            avg_duration_ms=("duration_ms", "mean"),
            median_duration_ms=("duration_ms", "median"),
            error_count=("is_error", "sum"),
        )
        .reset_index()
        .sort_values(["call_count", "tool_name"], ascending=[False, True])
    )
    summary["usage_pct"] = (summary["call_count"] / total_calls) * 100.0
    summary["error_rate_pct"] = (summary["error_count"] / summary["call_count"]) * 100.0
    for col in ["avg_duration_ms", "median_duration_ms", "usage_pct", "error_rate_pct"]:
        summary[col] = summary[col].fillna(0.0).astype(float)
    summary["error_count"] = summary["error_count"].astype(int)
    summary["call_count"] = summary["call_count"].astype(int)
    summary["unique_users"] = summary["unique_users"].astype(int)
    return _to_records(summary)


def _top_users(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    summary = (
        df.groupby("username", dropna=False)
        .agg(
            call_count=("username", "size"),
            unique_tools=("tool_name", pd.Series.nunique),
            avg_duration_ms=("duration_ms", "mean"),
            median_duration_ms=("duration_ms", "median"),
            error_count=("is_error", "sum"),
        )
        .reset_index()
        .sort_values(["call_count", "username"], ascending=[False, True])
    )
    summary["error_rate_pct"] = (summary["error_count"] / summary["call_count"]) * 100.0
    for col in ["avg_duration_ms", "median_duration_ms", "error_rate_pct"]:
        summary[col] = summary[col].fillna(0.0).astype(float)
    summary["call_count"] = summary["call_count"].astype(int)
    summary["unique_tools"] = summary["unique_tools"].astype(int)
    summary["error_count"] = summary["error_count"].astype(int)
    return _to_records(summary)


def _slowest_tools(df: pd.DataFrame, quantile: float, metric_name: str) -> list[dict[str, Any]]:
    if df.empty:
        return []

    summary = (
        df.groupby("tool_name", dropna=False)["duration_ms"]
        .quantile(quantile)
        .reset_index(name=metric_name)
        .sort_values([metric_name, "tool_name"], ascending=[False, True])
    )
    summary[metric_name] = summary[metric_name].fillna(0.0).astype(float)
    return _to_records(summary)


def _calls_by_day(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    summary = (
        df.dropna(subset=["date"])
        .groupby("date", dropna=False)
        .size()
        .reset_index(name="call_count")
        .sort_values("date")
    )
    summary["call_count"] = summary["call_count"].astype(int)
    return _to_records(summary)


def _calls_by_weekday(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    summary = (
        df.dropna(subset=["weekday"])
        .groupby("weekday", observed=False)
        .size()
        .reset_index(name="call_count")
        .sort_values("weekday")
    )
    summary["weekday"] = summary["weekday"].astype(str)
    summary["call_count"] = summary["call_count"].astype(int)
    return _to_records(summary)


def _calls_by_hour(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    summary = (
        df.dropna(subset=["hour"])
        .groupby("hour", dropna=False)
        .size()
        .reset_index(name="call_count")
        .sort_values("hour")
    )
    summary["hour"] = summary["hour"].astype(int)
    summary["call_count"] = summary["call_count"].astype(int)
    return _to_records(summary)


def _usage_heatmap(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    summary = (
        df.dropna(subset=["weekday", "hour"])
        .groupby(["weekday", "hour"], observed=False)
        .size()
        .reset_index(name="call_count")
        .sort_values(["weekday", "hour"])
    )
    summary["weekday"] = summary["weekday"].astype(str)
    summary["hour"] = summary["hour"].astype(int)
    summary["call_count"] = summary["call_count"].astype(int)
    return _to_records(summary)


def _top_error_messages(df: pd.DataFrame) -> list[dict[str, Any]]:
    error_rows = df[df.get("is_error", False)].copy()
    if error_rows.empty:
        return []
    summary = (
        error_rows.assign(error_message=error_rows["error_message"].fillna("Unknown error"))
        .groupby("error_message", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "error_message"], ascending=[False, True])
    )
    summary["count"] = summary["count"].astype(int)
    return _to_records(summary)


def build_idra_usage_metrics(tool_calls_df: pd.DataFrame) -> Dict[str, Any]:
    prepared = _prepare_dataframe(tool_calls_df)
    total_calls = int(len(prepared.index))
    unique_tools = int(prepared["tool_name"].nunique()) if "tool_name" in prepared.columns else 0
    unique_users = int(prepared["username"].nunique()) if "username" in prepared.columns else 0
    unique_days = int(prepared["date"].dropna().nunique()) if "date" in prepared.columns else 0
    duration_series = prepared["duration_ms"].dropna() if "duration_ms" in prepared.columns else pd.Series(dtype="float64")

    error_count = int(prepared["is_error"].sum()) if "is_error" in prepared.columns else 0
    success_count = int(prepared["is_success"].sum()) if "is_success" in prepared.columns else 0
    unknown_count = int(prepared["is_unknown"].sum()) if "is_unknown" in prepared.columns else 0

    calls_by_day = _calls_by_day(prepared)
    calls_by_weekday = _calls_by_weekday(prepared)
    calls_by_hour = _calls_by_hour(prepared)

    peak_day = max(calls_by_day, key=lambda row: (row["call_count"], row["date"])) if calls_by_day else None
    peak_hour = max(calls_by_hour, key=lambda row: (row["call_count"], row["hour"])) if calls_by_hour else None

    return {
        "summary": {
            "total_calls": total_calls,
            "unique_tools": unique_tools,
            "unique_users": unique_users,
            "avg_calls_per_day": float(total_calls / unique_days) if unique_days else 0.0,
        },
        "tools": {
            "top_used": _top_tools(prepared),
            "slowest_by_median": _slowest_tools(prepared, 0.50, "median_duration_ms"),
            "slowest_by_p95": _slowest_tools(prepared, 0.95, "p95_duration_ms"),
        },
        "users": {
            "top_active": _top_users(prepared),
        },
        "performance": {
            "avg_duration_ms": _safe_float(duration_series.mean()) if not duration_series.empty else 0.0,
            "median_duration_ms": _safe_float(duration_series.quantile(0.50)) if not duration_series.empty else 0.0,
            "p95_duration_ms": _safe_float(duration_series.quantile(0.95)) if not duration_series.empty else 0.0,
            "p99_duration_ms": _safe_float(duration_series.quantile(0.99)) if not duration_series.empty else 0.0,
        },
        "usage_patterns": {
            "calls_by_day": calls_by_day,
            "calls_by_weekday": calls_by_weekday,
            "calls_by_hour": calls_by_hour,
            "heatmap": _usage_heatmap(prepared),
            "peak_day": peak_day,
            "peak_hour": peak_hour,
        },
        "quality": {
            "success_count": success_count,
            "error_count": error_count,
            "unknown_count": unknown_count,
            "error_rate_pct": float((error_count / total_calls) * 100.0) if total_calls else 0.0,
            "top_error_messages": _top_error_messages(prepared),
            "errors_by_day": _calls_by_day(prepared[prepared["is_error"]]),
            "errors_by_hour": _calls_by_hour(prepared[prepared["is_error"]]),
            "response_kind_distribution": _distribution(prepared, "response_kind", "count"),
        },
    }

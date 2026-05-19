from typing import Any, Dict, Optional

import pandas as pd


DELIVERY_COMPLETED_STATUSES = {"Fully Billed", "Pending Bill"}


def normalize_topic(topic: Optional[str]) -> str:
    normalized = (topic or "vendors").strip().lower()
    if not normalized:
        return "vendors"
    if normalized not in {"vendors", "items"}:
        raise ValueError("Invalid topic. Allowed values: vendors, items")
    return normalized


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.to_dict(orient="records")


def _series_count(series: pd.Series, key: str, value_name: str = "count") -> list[dict[str, Any]]:
    counts = series.fillna("Unknown").value_counts(dropna=False).reset_index()
    counts.columns = [key, value_name]
    return _to_records(counts)


def _safe_pct(num: float, den: float) -> float:
    return float((num / den) * 100.0) if den else 0.0


def _build_line_receipt_score(quantity: pd.Series, quantity_received: pd.Series) -> pd.Series:
    quantity = pd.to_numeric(quantity, errors="coerce").fillna(0.0)
    quantity_received = pd.to_numeric(quantity_received, errors="coerce").fillna(0.0)

    score = pd.Series(0.0, index=quantity.index, dtype="float64")
    valid_quantity = quantity > 0
    score.loc[valid_quantity] = (quantity_received.loc[valid_quantity] / quantity.loc[valid_quantity]).clip(lower=0.0, upper=1.0)
    return score


def _prepare_dates(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    for col in ["receive_by", "new_receive_by", "last_est_delivery_date_informed", "today", "po_date"]:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors="coerce")
    return data


def _resolve_latest_commit_date(data: pd.DataFrame) -> pd.Series:
    date_columns = [
        col
        for col in ["receive_by", "new_receive_by", "last_est_delivery_date_informed"]
        if col in data.columns
    ]
    if not date_columns:
        return pd.Series(pd.NaT, index=data.index)
    return data[date_columns].max(axis=1)


def build_vendors_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "topic": "vendors",
            "overview": {},
            "status_analysis": {},
            "vendor_analysis": {},
            "subsidiary_analysis": {},
            "delivery_analysis": {},
            "rescheduling_analysis": {},
            "monthly_trend": {},
        }

    data = _prepare_dates(df)
    data["amount_usd"] = pd.to_numeric(data.get("amount_usd"), errors="coerce").fillna(0.0)
    df_po = data.drop_duplicates("po_id").copy()

    overview = {
        "total_purchase_orders": int(len(df_po)),
        "total_vendors": int(df_po["vendor"].nunique(dropna=True)),
        "total_customers": int(df_po["customer"].nunique(dropna=True)),
        "total_amount_usd": float(df_po["amount_usd"].sum()),
        "avg_amount_per_po": float(df_po["amount_usd"].mean() or 0.0),
    }

    status_amount = df_po.groupby("status", dropna=False)["amount_usd"].sum().reset_index()
    payment_amount = df_po.groupby("payment_status", dropna=False)["amount_usd"].sum().reset_index()
    status_analysis = {
        "status_distribution": _series_count(df_po["status"], "status"),
        "approval_status_distribution": _series_count(df_po["approval_status"], "approval_status"),
        "expediting_status_distribution": _series_count(df_po["expediting_status"], "expediting_status"),
        "payment_status_distribution": _series_count(df_po["payment_status"], "payment_status"),
        "amount_by_status": _to_records(status_amount.rename(columns={"amount_usd": "amount_usd"})),
        "amount_by_payment_status": _to_records(payment_amount.rename(columns={"amount_usd": "amount_usd"})),
    }

    by_vendor_count = df_po.groupby("vendor", dropna=False)["po_id"].nunique().reset_index(name="po_count")
    by_vendor_amount = df_po.groupby("vendor", dropna=False)["amount_usd"].sum().reset_index()
    country_amount = df_po.groupby("vendor_country", dropna=False)["amount_usd"].sum().reset_index()
    vendor_analysis = {
        "top_vendors_by_po_count": _to_records(by_vendor_count.sort_values("po_count", ascending=False).head(10)),
        "top_vendors_by_amount": _to_records(by_vendor_amount.sort_values("amount_usd", ascending=False).head(10)),
        "vendor_country_distribution": _series_count(df_po["vendor_country"], "vendor_country"),
        "amount_by_vendor_country": _to_records(country_amount),
        "terms_distribution": _series_count(df_po["terms"], "terms"),
        "incoterms_distribution": _series_count(df_po["incoterms"], "incoterms"),
    }

    sub_po = df_po.groupby("subsidiary", dropna=False)["po_id"].nunique().reset_index(name="po_count")
    sub_amount = df_po.groupby("subsidiary", dropna=False)["amount_usd"].sum().reset_index()
    sub_status = (
        df_po.groupby(["subsidiary", "status"], dropna=False)["po_id"]
        .nunique()
        .reset_index(name="po_count")
        .sort_values("po_count", ascending=False)
    )
    subsidiary_analysis = {
        "po_count_by_subsidiary": _to_records(sub_po),
        "amount_by_subsidiary": _to_records(sub_amount),
        "status_by_subsidiary": _to_records(sub_status),
    }

    df_po["effective_receive_by"] = _resolve_latest_commit_date(df_po)
    df_po["days_to_receive"] = (df_po["effective_receive_by"] - df_po["today"]).dt.days
    df_po["is_overdue"] = df_po["days_to_receive"] < 0

    delivery_df = df_po[~df_po["status"].isin(DELIVERY_COMPLETED_STATUSES)].copy()
    overdue = delivery_df[delivery_df["is_overdue"]]
    due_week = delivery_df[delivery_df["days_to_receive"].between(0, 7, inclusive="both")]
    due_30 = delivery_df[delivery_df["days_to_receive"].between(0, 30, inclusive="both")]
    delivery_analysis = {
        "overdue_pos": int(len(overdue)),
        "due_this_week": int(len(due_week)),
        "due_next_30_days": int(len(due_30)),
        "avg_days_to_receive": float(delivery_df["days_to_receive"].dropna().mean() or 0.0),
        "avg_overdue_days": float((-overdue["days_to_receive"]).dropna().mean() or 0.0),
        "overdue_amount_usd": float(overdue["amount_usd"].sum()),
        "overdue_by_vendor": _to_records(overdue.groupby("vendor", dropna=False)["po_id"].nunique().reset_index(name="po_count")),
        "overdue_by_country": _to_records(
            overdue.groupby("vendor_country", dropna=False)["po_id"].nunique().reset_index(name="po_count")
        ),
        "aging_buckets": {
            "overdue_30_plus": int((delivery_df["days_to_receive"] <= -30).sum()),
            "overdue_15_30": int(delivery_df["days_to_receive"].between(-30, -15, inclusive="right").sum()),
            "overdue_1_14": int(delivery_df["days_to_receive"].between(-14, -1, inclusive="both").sum()),
            "due_0_7": int(delivery_df["days_to_receive"].between(0, 7, inclusive="both").sum()),
            "due_8_30": int(delivery_df["days_to_receive"].between(8, 30, inclusive="both").sum()),
            "due_31_plus": int((delivery_df["days_to_receive"] >= 31).sum()),
        },
    }

    closed_df = df_po[df_po["status"].isin(DELIVERY_COMPLETED_STATUSES)].copy()
    closed_df["delay_days"] = (closed_df["effective_receive_by"] - closed_df["receive_by"]).dt.days
    closed_df["had_delay"] = closed_df["delay_days"].fillna(0) > 0
    closed_delayed = closed_df[closed_df["had_delay"]]
    closed_delivery_analysis = {
        "closed_pos_count": int(len(closed_df)),
        "closed_pos_with_delay": int(len(closed_delayed)),
        "closed_pos_with_delay_pct": _safe_pct(float(len(closed_delayed)), float(len(closed_df))),
        "avg_delay_days": float(closed_delayed["delay_days"].dropna().mean() or 0.0),
        "delayed_amount_usd": float(closed_delayed["amount_usd"].sum()),
        "delayed_pos_by_vendor": _to_records(
            closed_delayed.groupby("vendor", dropna=False)["po_id"].nunique().reset_index(name="po_count")
        ),
    }

    df_po["reschedule_days"] = (df_po["new_receive_by"] - df_po["receive_by"]).dt.days
    rescheduled = df_po[df_po["new_receive_by"].notna() & (df_po["new_receive_by"] != df_po["receive_by"])]
    rescheduling_analysis = {
        "rescheduled_po_count": int(len(rescheduled)),
        "rescheduled_po_pct": _safe_pct(float(len(rescheduled)), float(len(df_po))),
        "avg_rescheduled_days": float(rescheduled["reschedule_days"].dropna().mean() or 0.0),
        "rescheduled_amount_usd": float(rescheduled["amount_usd"].sum()),
        "top_vendors_by_reschedules": _to_records(
            rescheduled.groupby("vendor", dropna=False)["po_id"].nunique().reset_index(name="po_count").sort_values("po_count", ascending=False).head(10)
        ),
    }

    monthly = df_po.groupby("po_period", dropna=False).agg(po_count=("po_id", "nunique"), amount_usd=("amount_usd", "sum")).reset_index()
    monthly_trend = {"by_month": _to_records(monthly.sort_values("po_period"))}

    return {
        "topic": "vendors",
        "overview": overview,
        "status_analysis": status_analysis,
        "vendor_analysis": vendor_analysis,
        "subsidiary_analysis": subsidiary_analysis,
        "delivery_analysis": delivery_analysis,
        "closed_delivery_analysis": closed_delivery_analysis,
        "rescheduling_analysis": rescheduling_analysis,
        "monthly_trend": monthly_trend,
    }


def build_items_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "topic": "items",
            "overview": {},
            "receipt_analysis": {},
            "brand_analysis": {},
            "product_group_analysis": {},
            "brand_vendor_analysis": {},
            "brand_customer_analysis": {},
            "pending_analysis": {},
        }

    data = df.copy()
    for col in ["quantity", "quantity_received", "line_amount"]:
        data[col] = pd.to_numeric(data.get(col), errors="coerce").fillna(0.0)

    data["pending_quantity"] = (data["quantity"] - data["quantity_received"]).clip(lower=0)
    data["line_receipt_score"] = _build_line_receipt_score(data["quantity"], data["quantity_received"])

    total_quantity = float(data["quantity"].sum())
    total_pending = float(data["pending_quantity"].sum())

    overview = {
        "total_lines": int(len(data)),
        "total_brands": int(data["brand"].nunique(dropna=True)),
        "total_product_groups": int(data["product_group"].nunique(dropna=True)),
        "total_line_amount": float(data["line_amount"].sum()),
        "total_pending_quantity": total_pending,
    }

    data["receipt_status"] = "partially_received"
    data.loc[data["quantity_received"] <= 0, "receipt_status"] = "not_received"
    data.loc[data["pending_quantity"] <= 0, "receipt_status"] = "fully_received"

    receipt_analysis = {
        "avg_line_receipt_score": float(data["line_receipt_score"].mean() or 0.0),
        "avg_line_receipt_score_pct": float((data["line_receipt_score"].mean() or 0.0) * 100.0),
        "receipt_status_distribution": _series_count(data["receipt_status"], "receipt_status"),
        "pending_lines": int((data["pending_quantity"] > 0).sum()),
        "partially_received_lines": int((data["receipt_status"] == "partially_received").sum()),
        "fully_received_lines": int((data["receipt_status"] == "fully_received").sum()),
        "not_received_lines": int((data["receipt_status"] == "not_received").sum()),
        "secondary_pending_quantity": total_pending,
    }

    brand_group = (
        data.groupby("brand", dropna=False)
        .agg(
            line_count=("item", "count"),
            amount_usd=("line_amount", "sum"),
            quantity=("quantity", "sum"),
            quantity_received=("quantity_received", "sum"),
            pending_quantity=("pending_quantity", "sum"),
            avg_line_receipt_score=("line_receipt_score", "mean"),
        )
        .reset_index()
    )
    brand_group["avg_line_receipt_score_pct"] = brand_group["avg_line_receipt_score"].fillna(0.0) * 100.0
    brand_analysis = {
        "distribution": _to_records(brand_group[["brand", "line_count"]]),
        "metrics": _to_records(brand_group),
    }

    pg_group = (
        data.groupby("product_group", dropna=False)
        .agg(
            line_count=("item", "count"),
            amount_usd=("line_amount", "sum"),
            quantity=("quantity", "sum"),
            quantity_received=("quantity_received", "sum"),
            pending_quantity=("pending_quantity", "sum"),
            avg_line_receipt_score=("line_receipt_score", "mean"),
        )
        .reset_index()
    )
    pg_group["avg_line_receipt_score_pct"] = pg_group["avg_line_receipt_score"].fillna(0.0) * 100.0
    product_group_analysis = {
        "distribution": _to_records(pg_group[["product_group", "line_count"]]),
        "metrics": _to_records(pg_group),
    }

    by_vendor_brand = (
        data.groupby(["vendor", "brand"], dropna=False)
        .agg(
            line_count=("item", "count"),
            amount_usd=("line_amount", "sum"),
            pending_quantity=("pending_quantity", "sum"),
            quantity=("quantity", "sum"),
            quantity_received=("quantity_received", "sum"),
            avg_line_receipt_score=("line_receipt_score", "mean"),
        )
        .reset_index()
    )
    by_vendor_brand["avg_line_receipt_score_pct"] = by_vendor_brand["avg_line_receipt_score"].fillna(0.0) * 100.0
    brand_vendor_analysis = {
        "distribution": _to_records(by_vendor_brand[["vendor", "brand", "line_count"]]),
        "top_by_amount": _to_records(by_vendor_brand.sort_values("amount_usd", ascending=False).head(10)),
        "top_by_pending_quantity": _to_records(by_vendor_brand.sort_values("pending_quantity", ascending=False).head(10)),
    }

    by_customer_brand = (
        data.groupby(["customer", "brand"], dropna=False)
        .agg(
            line_count=("item", "count"),
            amount_usd=("line_amount", "sum"),
            pending_quantity=("pending_quantity", "sum"),
            quantity=("quantity", "sum"),
            quantity_received=("quantity_received", "sum"),
            avg_line_receipt_score=("line_receipt_score", "mean"),
        )
        .reset_index()
    )
    by_customer_brand["avg_line_receipt_score_pct"] = by_customer_brand["avg_line_receipt_score"].fillna(0.0) * 100.0
    brand_customer_analysis = {
        "distribution": _to_records(by_customer_brand[["customer", "brand", "line_count"]]),
        "top_by_amount": _to_records(by_customer_brand.sort_values("amount_usd", ascending=False).head(10)),
        "top_by_pending_quantity": _to_records(by_customer_brand.sort_values("pending_quantity", ascending=False).head(10)),
    }

    pending_only = data[data["pending_quantity"] > 0]
    pending_analysis = {
        "pending_amount_usd": float(pending_only["line_amount"].sum()),
        "pending_quantity_by_brand": _to_records(
            pending_only.groupby("brand", dropna=False)["pending_quantity"].sum().reset_index()
        ),
        "pending_quantity_by_product_group": _to_records(
            pending_only.groupby("product_group", dropna=False)["pending_quantity"].sum().reset_index()
        ),
        "pending_quantity_by_vendor": _to_records(
            pending_only.groupby("vendor", dropna=False)["pending_quantity"].sum().reset_index()
        ),
        "pending_quantity_by_customer": _to_records(
            pending_only.groupby("customer", dropna=False)["pending_quantity"].sum().reset_index()
        ),
        "pending_amount_by_brand": _to_records(pending_only.groupby("brand", dropna=False)["line_amount"].sum().reset_index()),
        "pending_amount_by_vendor": _to_records(
            pending_only.groupby("vendor", dropna=False)["line_amount"].sum().reset_index()
        ),
        "pending_amount_by_customer": _to_records(
            pending_only.groupby("customer", dropna=False)["line_amount"].sum().reset_index()
        ),
    }

    return {
        "topic": "items",
        "overview": overview,
        "receipt_analysis": receipt_analysis,
        "brand_analysis": brand_analysis,
        "product_group_analysis": product_group_analysis,
        "brand_vendor_analysis": brand_vendor_analysis,
        "brand_customer_analysis": brand_customer_analysis,
        "pending_analysis": pending_analysis,
    }

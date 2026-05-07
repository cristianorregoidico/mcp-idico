from typing import Any, Dict

import numpy as np
import pandas as pd


def on_time_delivery_summary(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()
    df["item_name_so"] = df["item_name_so"].replace("", np.nan)
    df["if_create_date"] = pd.to_datetime(df["if_create_date"], errors="coerce")
    df["if_month"] = df["if_create_date"].dt.to_period("M")
    df_valid = df.dropna(subset=["if_month", "item_name_so"])

    total_counts = df_valid.groupby("if_month")["item_name_so"].count().rename("total_items_delivery")
    on_time_counts = (
        df_valid.loc[df_valid["delivery_status"] == "On Time"]
        .groupby("if_month")["item_name_so"]
        .count()
        .rename("items_on_time")
    )
    total_so_delivery = int(df_valid["so_doc_number"].nunique())

    result = pd.concat([total_counts, on_time_counts], axis=1).fillna(0).reset_index()
    result["total_items_delivery"] = result["total_items_delivery"].astype(int)
    result["items_on_time"] = result["items_on_time"].astype(int)
    result["on_time_pct"] = np.where(
        result["total_items_delivery"] > 0,
        result["items_on_time"] / result["total_items_delivery"],
        0.0,
    )
    result["month"] = result["if_month"].astype(str)
    by_month = result[["month", "total_items_delivery", "items_on_time", "on_time_pct"]].to_dict(orient="records")

    overall_total = int(len(df_valid))
    overall_on_time = int((df_valid["delivery_status"] == "On Time").sum())
    overall = {
        "total_items_delivery": overall_total,
        "items_on_time": overall_on_time,
        "on_time_pct": float(overall_on_time / overall_total) if overall_total > 0 else 0.0,
    }

    po_status_distribution: Dict[str, int] = {}
    if {"po_doc_number", "po_status"}.issubset(df.columns):
        df_po = df.copy()
        df_po["po_status_clean"] = df_po["po_status"].fillna("None")
        status_grp = df_po.groupby("po_status_clean", dropna=False)["po_doc_number"].nunique().reset_index(name="count")
        for _, row in status_grp.iterrows():
            po_status_distribution[str(row["po_status_clean"])] = int(row["count"])

    return {
        "on_time_delivery": {"by_month": by_month, "overall": overall},
        "total_so_delivery": total_so_delivery,
        "po_status_distribution": po_status_distribution,
        "so_details": None,
    }

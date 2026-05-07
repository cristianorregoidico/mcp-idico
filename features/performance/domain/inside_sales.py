import numpy as np
import pandas as pd


def analyze_inside_sales(df: pd.DataFrame) -> dict:
    df = df.copy()
    for col in ["op_date", "q_date", "so_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["q_amount", "so_amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    total_opps = int(len(df))
    total_quotes = int(df["q_number"].notna().sum())
    total_sos = int(df["so_number"].notna().sum())
    hitrate_o_q_vol = total_quotes / total_opps if total_opps > 0 else np.nan
    quotes_with_so = int(df[df["q_number"].notna() & df["so_number"].notna()].shape[0])
    hitrate_q_so_vol = quotes_with_so / total_quotes if total_quotes > 0 else np.nan
    total_q_amount = df.loc[df["q_number"].notna(), "q_amount"].sum(min_count=1)
    converted_so_amount = df.loc[df["q_number"].notna() & df["so_number"].notna(), "so_amount"].sum(min_count=1)
    hitrate_q_so_amt = converted_so_amount / total_q_amount if pd.notna(total_q_amount) and total_q_amount > 0 else np.nan

    to_float = lambda x: float(x) if pd.notna(x) else None
    summary = {
        "total_opportunities": total_opps,
        "total_quotes": total_quotes,
        "total_sales_orders": total_sos,
        "hitrate_opportunity_to_quote_volume": to_float(hitrate_o_q_vol),
        "hitrate_quote_to_so_volume": to_float(hitrate_q_so_vol),
        "hitrate_quote_to_so_amount": to_float(hitrate_q_so_amt),
    }

    mask_resp = df["op_date"].notna() & df["q_date"].notna()
    df_resp = df.loc[mask_resp].copy()
    if not df_resp.empty:
        df_resp["response_time_days"] = (df_resp["q_date"] - df_resp["op_date"]).dt.total_seconds() / 86400.0
        response_time = {
            "overall": {
                "count": int(df_resp.shape[0]),
                "avg_days": to_float(df_resp["response_time_days"].mean()),
                "median_days": to_float(df_resp["response_time_days"].median()),
                "p90_days": to_float(df_resp["response_time_days"].quantile(0.9)),
            },
            "by_inside_sales": [
                {
                    "inside_sales": row["inside_sales"],
                    "count": int(row["count"]),
                    "avg_days": to_float(row["avg_days"]),
                    "median_days": to_float(row["median_days"]),
                }
                for _, row in df_resp.groupby("inside_sales")["response_time_days"].agg(["count", "mean", "median"]).reset_index().rename(columns={"mean": "avg_days", "median": "median_days"}).iterrows()
            ],
        }
    else:
        response_time = {"overall": None, "by_inside_sales": []}

    hitrates = {
        "opportunity_to_quote": {"volume": {"total_opportunities": total_opps, "with_quote": total_quotes, "hitrate": to_float(hitrate_o_q_vol)}},
        "quote_to_sales_order": {
            "volume": {"total_quotes": total_quotes, "with_sales_order": quotes_with_so, "hitrate": to_float(hitrate_q_so_vol)},
            "amount": {"total_quote_amount": to_float(total_q_amount), "converted_so_amount": to_float(converted_so_amount), "hitrate": to_float(hitrate_q_so_amt)},
        },
    }

    return {
        "summary": summary,
        "response_time": response_time,
        "hitrates": hitrates,
        "full_data_reference": "dataset_reference",
    }

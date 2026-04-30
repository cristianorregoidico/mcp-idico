import pandas as pd
import numpy as np


def analyze_inside_sales(df: pd.DataFrame) -> dict:
    """
    Analiza desempeño de Inside Sales a partir de un dataset con columnas:
    ['op_number', 'op_date', 'op_status', 'inside_sales', 'customer',
     'q_number', 'q_date', 'q_status', 'q_amount',
     'so_number', 'so_date', 'so_status', 'so_amount']
    
    Retorna un dict (JSON-serializable) con:
        - summary: métricas globales (sin texto natural)
        - response_time: métricas de tiempo de respuesta Oportunidad → Quote
        - hitrates: KPIs de conversión O→Q (volumen) y Q→SO (volumen y monto)
        - scorecard_inside_sales: ranking por Inside Sales
    """

    df = df.copy()

    # ------------------------------
    # Normalización de tipos de datos (solo uso interno)
    # ------------------------------
    date_cols = ["op_date", "q_date", "so_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Asegurar que montos sean numéricos
    for col in ["q_amount", "so_amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------
    # 1. MÉTRICAS GLOBALES (summary numérico)
    # ------------------------------
    total_opps = int(len(df))
    total_quotes = int(df["q_number"].notna().sum())
    total_sos = int(df["so_number"].notna().sum())

    # Hitrate Oportunidad → Quote (volumen)
    if total_opps > 0:
        hitrate_o_q_vol = total_quotes / total_opps
    else:
        hitrate_o_q_vol = np.nan

    # Hitrate Quote → SO (volumen)
    total_quotes_for_hitrate = total_quotes
    quotes_with_so = int(df[df["q_number"].notna() & df["so_number"].notna()].shape[0])

    if total_quotes_for_hitrate > 0:
        hitrate_q_so_vol = quotes_with_so / total_quotes_for_hitrate
    else:
        hitrate_q_so_vol = np.nan

    # Hitrate Quote → SO (monto)
    total_q_amount = df.loc[df["q_number"].notna(), "q_amount"].sum(min_count=1)
    converted_so_amount = df.loc[
        df["q_number"].notna() & df["so_number"].notna(), "so_amount"
    ].sum(min_count=1)

    if pd.notna(total_q_amount) and total_q_amount > 0:
        hitrate_q_so_amt = converted_so_amount / total_q_amount
    else:
        hitrate_q_so_amt = np.nan

    def _to_float_or_none(x):
        return float(x) if pd.notna(x) else None

    summary = {
        "total_opportunities": total_opps,
        "total_quotes": total_quotes,
        "total_sales_orders": total_sos,
        "hitrate_opportunity_to_quote_volume": _to_float_or_none(hitrate_o_q_vol),
        "hitrate_quote_to_so_volume": _to_float_or_none(hitrate_q_so_vol),
        "hitrate_quote_to_so_amount": _to_float_or_none(hitrate_q_so_amt),
    }

    # ------------------------------
    # 2. TIEMPO DE RESPUESTA OPORTUNIDAD → QUOTE
    # ------------------------------
    mask_resp = df["op_date"].notna() & df["q_date"].notna()
    df_resp = df.loc[mask_resp].copy()

    if not df_resp.empty:
        df_resp["response_time_days"] = (
            df_resp["q_date"] - df_resp["op_date"]
        ).dt.total_seconds() / 86400.0

        overall_response_time = {
            "count": int(df_resp.shape[0]),
            "avg_days": _to_float_or_none(df_resp["response_time_days"].mean()),
            "median_days": _to_float_or_none(df_resp["response_time_days"].median()),
            "p90_days": _to_float_or_none(df_resp["response_time_days"].quantile(0.9)),
        }

        by_is = (
            df_resp.groupby("inside_sales")["response_time_days"]
            .agg(["count", "mean", "median"])
            .reset_index()
        )
        by_is = by_is.rename(columns={"mean": "avg_days", "median": "median_days"})

        response_time_by_inside = []
        for _, row in by_is.iterrows():
            response_time_by_inside.append(
                {
                    "inside_sales": row["inside_sales"],
                    "count": int(row["count"]),
                    "avg_days": _to_float_or_none(row["avg_days"]),
                    "median_days": _to_float_or_none(row["median_days"]),
                }
            )

        response_time = {
            "overall": overall_response_time,
            "by_inside_sales": response_time_by_inside,
        }
    else:
        response_time = {
            "overall": None,
            "by_inside_sales": [],
        }

    # ------------------------------
    # 3. HITRATES DETALLADOS
    # ------------------------------
    hitrates = {
        "opportunity_to_quote": {
            "volume": {
                "total_opportunities": total_opps,
                "with_quote": total_quotes,
                "hitrate": _to_float_or_none(hitrate_o_q_vol),
            }
        },
        "quote_to_sales_order": {
            "volume": {
                "total_quotes": total_quotes_for_hitrate,
                "with_sales_order": quotes_with_so,
                "hitrate": _to_float_or_none(hitrate_q_so_vol),
            },
            "amount": {
                "total_quote_amount": _to_float_or_none(total_q_amount),
                "converted_so_amount": _to_float_or_none(converted_so_amount),
                "hitrate": _to_float_or_none(hitrate_q_so_amt),
            },
        },
    }

    # ------------------------------
    # 4. SCORECARD / RANKING POR INSIDE SALES
    # ------------------------------
    def n_notna(series):
        return int(series.notna().sum())

    base = (
        df.groupby("inside_sales")
        .agg(
            total_opportunities=("op_number", "count"),
            total_quotes=("q_number", n_notna),
            total_sos=("so_number", n_notna),
            total_q_amount=("q_amount", "sum"),
            total_so_amount=("so_amount", "sum"),
        )
        .reset_index()
    )

    # Hitrates por Inside Sales
    base["hitrate_op_q_volume"] = np.where(
        base["total_opportunities"] > 0,
        base["total_quotes"] / base["total_opportunities"],
        np.nan,
    )

    base["hitrate_q_so_volume"] = np.where(
        base["total_quotes"] > 0,
        base["total_sos"] / base["total_quotes"],
        np.nan,
    )

    base["hitrate_q_so_amount"] = np.where(
        base["total_q_amount"] > 0,
        base["total_so_amount"] / base["total_q_amount"],
        np.nan,
    )

    # Tiempos de respuesta por Inside Sales
    if not df_resp.empty:
        rt_by_is = (
            df_resp.groupby("inside_sales")["response_time_days"]
            .mean()
            .rename("avg_response_time_days")
            .reset_index()
        )
    else:
        rt_by_is = pd.DataFrame(columns=["inside_sales", "avg_response_time_days"])

    base = base.merge(rt_by_is, on="inside_sales", how="left")

    # Normalización para score
    def min_max_norm(series):
        s = series.astype(float)
        if s.notna().sum() == 0:
            return pd.Series([np.nan] * len(s), index=s.index)
        min_v = s.min()
        max_v = s.max()
        if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
            return pd.Series([1.0] * len(s), index=s.index)
        return (s - min_v) / (max_v - min_v)

    base["norm_hitrate_op_q_volume"] = min_max_norm(base["hitrate_op_q_volume"])
    base["norm_hitrate_q_so_volume"] = min_max_norm(base["hitrate_q_so_volume"])
    base["norm_hitrate_q_so_amount"] = min_max_norm(base["hitrate_q_so_amount"])

    base["norm_response_time"] = min_max_norm(base["avg_response_time_days"])
    if base["norm_response_time"].notna().any():
        base["norm_response_time_inv"] = 1 - base["norm_response_time"]
    else:
        base["norm_response_time_inv"] = np.nan

    # Pesos del score
    w_op_q = 0.2
    w_vol = 0.3
    w_amt = 0.4
    w_rt = 0.1

    base["score"] = (
        w_op_q * base["norm_hitrate_op_q_volume"].fillna(0)
        + w_vol * base["norm_hitrate_q_so_volume"].fillna(0)
        + w_amt * base["norm_hitrate_q_so_amount"].fillna(0)
        + w_rt * base["norm_response_time_inv"].fillna(0)
    ) * 100.0

    base_sorted = base.sort_values("score", ascending=False)

    scorecard_list = []
    for _, row in base_sorted.iterrows():
        scorecard_list.append(
            {
                "inside_sales": row["inside_sales"],
                "total_opportunities": int(row["total_opportunities"]),
                "total_quotes": int(row["total_quotes"]),
                "total_sos": int(row["total_sos"]),
                "total_q_amount": _to_float_or_none(row["total_q_amount"]),
                "total_so_amount": _to_float_or_none(row["total_so_amount"]),
                "hitrate_op_q_volume": _to_float_or_none(row["hitrate_op_q_volume"]),
                "hitrate_q_so_volume": _to_float_or_none(row["hitrate_q_so_volume"]),
                "hitrate_q_so_amount": _to_float_or_none(row["hitrate_q_so_amount"]),
                "avg_response_time_days": _to_float_or_none(
                    row["avg_response_time_days"]
                ),
                "score": _to_float_or_none(row["score"]),
            }
        )

    result = {
        "summary": summary,
        "response_time": response_time,
        "hitrates": hitrates,
        "full_data_reference": "dataset_reference"
    }

    return result
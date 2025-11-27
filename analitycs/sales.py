import pandas as pd
import numpy as np
import json
from typing import Dict, Any

def finance_summary(df: pd.DataFrame) -> dict:
    """
    df: DataFrame con columnas al menos:
        ['so_number','date','customer','customer_country','sales_rep',
         'gross_usd','net_usd','terms','gross_margin','gross_margin_pct']
    """

    # Asegurar tipos
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # -----------------------------
    # 1) PERIODO
    # -----------------------------
    if "date" in df.columns and df["date"].notna().any():
        start_date = df["date"].min().date().isoformat()
        end_date = df["date"].max().date().isoformat()
    else:
        start_date = None
        end_date = None

    # -----------------------------
    # 2) BOOKINGS
    # -----------------------------
    total_bookings = float(df["net_usd"].sum()) if len(df) > 0 else 0.0
    order_count = int(len(df))
    average_booking = float(df["net_usd"].mean()) if order_count > 0 else 0.0

    # Bookings por país
    bookings_by_country = (
        df.groupby("customer_country")["net_usd"].sum()
        .to_dict()
    )

    # Bookings por sales rep
    bookings_by_sales_rep = (
        df.groupby("sales_rep")["net_usd"].sum()
        .to_dict()
    )

    # Forzar float en los diccionarios
    bookings_by_country = {k: float(v) for k, v in bookings_by_country.items()}
    bookings_by_sales_rep = {k: float(v) for k, v in bookings_by_sales_rep.items()}

    # -----------------------------
    # 3) GROSS MARGIN
    # -----------------------------
    gross_profit_total = float(df["gross_margin"].sum()) if "gross_margin" in df else 0.0

    if "gross_margin_pct" in df:
        average_gm_pct = float(df["gross_margin_pct"].mean()) if order_count > 0 else 0.0
    else:
        average_gm_pct = 0.0

    if "gross_usd" in df and df["gross_usd"].sum() != 0:
        weighted_gm_pct = float(df["gross_margin"].sum() / df["gross_usd"].sum())
    else:
        weighted_gm_pct = 0.0

    # ---------- 3.a) MARGIN BUCKETS ----------
    margin_bucket_summary = []
    if {"gross_usd", "gross_margin", "customer"}.issubset(df.columns):
        df_mb = df[df["gross_usd"] > 0].copy()

        if not df_mb.empty:
            # Agregar por cliente para calcular su GM% ponderado
            cust = df_mb.groupby("customer").agg(
                gross_usd_sum=("gross_usd", "sum"),
                gross_margin_sum=("gross_margin", "sum"),
            )
            cust["gm_pct"] = cust["gross_margin_sum"] / cust["gross_usd_sum"]

            def bucket(p):
                if p < 0.10:
                    return "0-10%"
                elif p < 0.20:
                    return "10-20%"
                else:
                    return "20%+"

            cust["margin_bucket"] = cust["gm_pct"].apply(bucket)

            bucket_agg = (
                cust.groupby("margin_bucket")
                .agg(
                    num_customers=("gm_pct", "size"),
                    gross_usd_sum=("gross_usd_sum", "sum"),
                )
                .reset_index()
            )

            margin_bucket_summary = [
                {
                    "margin_bucket": str(row["margin_bucket"]),
                    "num_customers": int(row["num_customers"]),
                    "gross_usd_sum": float(row["gross_usd_sum"]),
                }
                for _, row in bucket_agg.iterrows()
            ]


    # -----------------------------
    # 4) TERMS
    # -----------------------------
    # Conteo de términos (incluyendo None como "None")
    terms_counts = df["terms"].value_counts(dropna=False).to_dict()
    terms_counts = {k if k is not None else "None": int(v)
                    for k, v in terms_counts.items()}

    # Porcentaje por término
    if order_count > 0:
        terms_pct = {
            term: round((count / order_count) * 100, 2)
            for term, count in terms_counts.items()
        }
    else:
        terms_pct = {term: 0.0 for term in terms_counts.keys()}

    # Bookings por término (rellenando None como "None")
    df_terms = df.copy()
    df_terms["terms_clean"] = df_terms["terms"].fillna("None")
    bookings_by_terms = (
        df_terms.groupby("terms_clean")["net_usd"].sum()
        .to_dict()
    )
    bookings_by_terms = {k: float(v) for k, v in bookings_by_terms.items()}

    # -----------------------------
    # 5) TOP CLIENTES & CONCENTRACIÓN
    # -----------------------------
    top_n = 3
    top_clients_series = (
        df.groupby("customer")["net_usd"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
    )

    top_clients = [
        {"client": client, "amount": float(amount)}
        for client, amount in top_clients_series.items()
    ]

    top_clients_share_amount = float(top_clients_series.sum()) if order_count > 0 else 0.0
    top_clients_share_pct = (
        float(top_clients_share_amount / total_bookings)
        if total_bookings > 0 else 0.0
    )

    kpi_by_subsidiary = []
    if {"subsidiary", "period"}.issubset(df.columns):
        grp = df.groupby(["period", "subsidiary"])

        for (period, subsidiary), g in grp:
            gross_usd = float(g["gross_usd"].sum()) if "gross_usd" in g else 0.0
            net_usd = float(g["net_usd"].sum()) if "net_usd" in g else 0.0
            gross_margin = float(g["gross_margin"].sum()) if "gross_margin" in g else 0.0

            if "gross_margin_pct" in g and len(g) > 0:
                gm_pct_prom = float(g["gross_margin_pct"].mean())
            else:
                gm_pct_prom = 0.0

            customers = int(g["customer"].nunique()) if "customer" in g else 0
            transactions = int(len(g))

            if "gross_margin" in g and "gross_usd" in g and gross_usd != 0:
                gross_margin_pct_weighted = float(g["gross_margin"].sum() / gross_usd)
            else:
                gross_margin_pct_weighted = 0.0

            kpi_by_subsidiary.append({
                "period": period,
                "subsidiary": subsidiary,
                "gross_usd": gross_usd,
                "net_usd": net_usd,
                "gross_margin": gross_margin,
                "gm_pct_prom": gm_pct_prom,
                "customers": customers,
                "transactions": transactions,
                "gross_margin_pct_weighted": gross_margin_pct_weighted
            })

    # -----------------------------
    # 7) DATA SAMPLE (primer y último registro)
    # -----------------------------
    data_sample = []
    if order_count > 0:
        first_row = df.iloc[0].copy()
        last_row = df.iloc[-1].copy()

        # Convertir a tipos serializables
        for row in (first_row, last_row):
            for col in row.index:
                val = row[col]
                if isinstance(val, (np.integer, np.floating)):
                    row[col] = float(val)
                elif isinstance(val, pd.Timestamp):
                    row[col] = val.date().isoformat()

        data_sample = [first_row.to_dict(), last_row.to_dict()]

    # -----------------------------
    # 8) ARMAR JSON FINAL
    # -----------------------------
    output = {
        "finance_summary": {
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "bookings": {
                "total_bookings": total_bookings,
                "order_count": order_count,
                "average_booking": average_booking,
                "bookings_by_country": bookings_by_country,
                "bookings_by_sales_rep": bookings_by_sales_rep
            },
            "gross_margin": {
                "gross_profit_total": gross_profit_total,
                "average_gm_pct": average_gm_pct,
                "weighted_gm_pct": weighted_gm_pct,
                "margin_buckets": margin_bucket_summary
            },
            "terms": {
                "distribution_count": terms_counts,
                "distribution_pct": terms_pct,
                "bookings_by_terms": bookings_by_terms
            },
            "top_clients": top_clients,
            "concentration": {
                "top_n": top_n,
                "top_clients_share_amount": top_clients_share_amount,
                "top_clients_share_pct": top_clients_share_pct
            },
            "kpi_by_subsidiary": kpi_by_subsidiary
        },
        "data_sample": data_sample,
        "full_data_reference": "full_data_reference"
    }

    return output

def opportunity_summary(df: pd.DataFrame) -> dict:
    """
    df: DataFrame con columnas:
        ['id','op_number','tran_date','expected_close_date',
         'customer','subsidiary','status','inside_sales']
    """

    # Asegurar tipo fecha
    df["tran_date"] = pd.to_datetime(df["tran_date"], errors="coerce")
    df_valid = df.dropna(subset=["tran_date"])

    # -----------------------------
    # 1) PERIODO
    # -----------------------------
    start_ts = df_valid["tran_date"].min()
    end_ts   = df_valid["tran_date"].max()

    start_date_obj = start_ts.date()
    end_date_obj   = end_ts.date()

    start_date = start_date_obj.isoformat()
    end_date   = end_date_obj.isoformat()

    today = pd.Timestamp.today().date()

    # ¿Misma semana ISO?
    start_iso = start_date_obj.isocalendar()
    end_iso   = end_date_obj.isocalendar()
    same_week = (start_iso.year == end_iso.year) and (start_iso.week == end_iso.week)

    # -----------------------------
    # 2) PERFORMANCE DE OPORTUNIDADES
    #    (siempre calculamos todo, pero mostramos según regla)
    # -----------------------------

    # --- Por día ---
    daily_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.date])
        .size()
        .reset_index(name="count")
    )
    daily_counts.columns = ["inside_sales", "date", "count"]
    daily_counts["date"] = daily_counts["date"].astype(str)
    low_daily_all = daily_counts[daily_counts["count"] < 4]

    # --- Por semana ---
    weekly_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.to_period("W")])
        .size()
        .reset_index(name="count")
    )
    weekly_counts.columns = ["inside_sales", "week", "count"]
    weekly_counts["week"] = weekly_counts["week"].astype(str)
    low_weekly_all = weekly_counts[weekly_counts["count"] < 15]

    # --- Por mes ---
    monthly_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.to_period("M")])
        .size()
        .reset_index(name="count")
    )
    monthly_counts.columns = ["inside_sales", "month", "count"]
    monthly_counts["month"] = monthly_counts["month"].astype(str)
    low_monthly_all = monthly_counts[monthly_counts["count"] < 50]

    # -----------------------------
    # 3) REGLA DE QUÉ MOSTRAR
    # -----------------------------
    # Caso 1: mismo día y es hoy -> solo daily
    if (start_date_obj == end_date_obj) and (start_date_obj == today):
        low_daily   = low_daily_all
        low_weekly  = pd.DataFrame(columns=["inside_sales", "week", "count"])
        low_monthly = pd.DataFrame(columns=["inside_sales", "month", "count"])

    # Caso 2: fechas distintas pero misma semana -> solo weekly
    elif (start_date_obj != end_date_obj) and same_week:
        low_daily   = pd.DataFrame(columns=["inside_sales", "date", "count"])
        low_weekly  = low_weekly_all
        low_monthly = pd.DataFrame(columns=["inside_sales", "month", "count"])

    # Caso 3: resto -> trabajar por mes (mismo mes o meses distintos)
    else:
        low_daily   = pd.DataFrame(columns=["inside_sales", "date", "count"])
        low_weekly  = pd.DataFrame(columns=["inside_sales", "week", "count"])
        low_monthly = low_monthly_all
        # Aquí, si hay varios meses, saldrán como:
        # month: "2025-10", "2025-11", etc.

    low_performance_indicators = {
        "daily":   low_daily.to_dict(orient="records"),
        "weekly":  low_weekly.to_dict(orient="records"),
        "monthly": low_monthly.to_dict(orient="records"),
    }

     # Distribución por cliente
    dist_customer = (
        df.groupby("customer").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Distribución por inside
    dist_inside = (
        df.groupby("inside_sales").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Distribución por estado
    dist_status = (
        df.groupby("status").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Oportunidades con 2+ días sin cotización
    df_valid = df.copy()
    df_valid["tran_date"] = pd.to_datetime(df_valid["tran_date"], errors="coerce")

    today = pd.Timestamp.today().normalize()
    df_valid["days_open"] = (today - df_valid["tran_date"]).dt.days

    overdue = df_valid[
        (df_valid["status"] == "In Progress") &
        (df_valid["days_open"] >= 2)
    ].copy()

    overdue["tran_date"] = overdue["tran_date"].dt.date.astype(str)

    overdue_list = overdue.head(10).to_dict(orient="records")
    
    total_opportunities = len(df_valid)
    total_customers = df_valid["customer"].nunique()

    customer_counts = (
        df_valid.groupby("customer")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    customer_counts["participation_pct"] = (
        customer_counts["count"] / total_opportunities * 100
    ).round(2)
    customer_participation_top10 = customer_counts.head(10).to_dict(orient="records")
    summary = {
        "total_opportunities": total_opportunities,
        "total_unique_customers": total_customers,
        "customer_participation": customer_participation_top10
    }
    # -----------------------------
    # 8) ARMAR JSON FINAL
    # -----------------------------
    output = {
            "period": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "summary": summary,
            "distribution": {
                "inside_sales": dist_inside,
                "status": dist_status
            },
            "overdue_in_progress": overdue_list,
            "low_performance_indicators": low_performance_indicators,
            "full_data_reference": "dataset_reference"
    }

    return output

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

def summarize_sold_items(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Espera un DataFrame con al menos las columnas:
    ['customer', 'quote', 'status', 'date', 'inside_sales',
     'item', 'item_description', 'brand', 'product_group',
     'selected_vendor', 'qty', 'unit_price', 'unit_cost',
     'gross_margin_pct']

    Retorna un dict con el shape:

    {
      "general_summary": {},
      "top_items": {
        "by_volume": [],
        "by_amount": [],
        "by_margin_amount": [],
        "by_margin_pct": []
      },
      "problematic_items": [],
      "vendor_summary": [],
      "distribution": {
        "by_brand": [],
        "by_product_group": []
      }
    }
    """

    df = df.copy()

    # ---------------------------
    # 0) Tipos y columnas base
    # ---------------------------
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Ventas de línea
    df["line_sales"] = df["qty"] * df["unit_price"]

    # Costo de línea: prioridad unit_cost, luego gross_margin_pct
    has_unit_cost = df["unit_cost"].notna() if "unit_cost" in df.columns else False

    df["line_cost_from_unit"] = np.where(
        has_unit_cost,
        df["qty"] * df["unit_cost"],
        np.nan
    )

    df["line_cost_from_margin_pct"] = np.where(
        (~has_unit_cost) & df["gross_margin_pct"].notna(),
        df["line_sales"] * (1 - df["gross_margin_pct"]),
        np.nan
    )

    df["line_cost"] = df["line_cost_from_unit"].fillna(df["line_cost_from_margin_pct"])
    df["line_gm"] = df["line_sales"] - df["line_cost"]
    df["line_gm"] = df["line_gm"].fillna(0.0)
    df["line_sales"] = df["line_sales"].fillna(0.0)

    # ---------------------------
    # 1) GENERAL SUMMARY
    # ---------------------------
    total_qty = float(df["qty"].sum())
    total_sales = float(df["line_sales"].sum())
    total_gm = float(df["line_gm"].sum())

    if total_sales > 0:
        avg_gm_pct = total_gm / total_sales
    else:
        avg_gm_pct = 0.0

    unique_customers = int(df["customer"].nunique())
    num_orders = int(df["quote"].nunique())

    general_summary = {
        "total_quantity": round(total_qty, 4),
        "total_sales": round(total_sales, 4),
        "total_gross_margin": round(total_gm, 4),
        "average_gross_margin_pct": round(avg_gm_pct, 6),
        "unique_customers": unique_customers,
        "number_of_orders": num_orders
    }

    # ---------------------------
    # 2) RESUMEN POR ITEM
    # ---------------------------
    item_group_cols = ["item", "item_description", "brand", "product_group"]
    item_group = df.groupby(item_group_cols, dropna=False).agg(
        total_qty=("qty", "sum"),
        total_sales=("line_sales", "sum"),
        total_gm=("line_gm", "sum"),
        avg_gm_pct_raw=("gross_margin_pct", "mean"),
        num_customers=("customer", "nunique"),
        num_orders=("quote", "nunique")
    ).reset_index()

    # Margen % ponderado por ventas
    item_group["avg_gm_pct_weighted"] = np.where(
        item_group["total_sales"] > 0,
        item_group["total_gm"] / item_group["total_sales"],
        np.nan
    )

    # Usamos primero weighted, si no, el promedio simple
    item_group["avg_gm_pct"] = (
        item_group["avg_gm_pct_weighted"]
        .fillna(item_group["avg_gm_pct_raw"])
        .fillna(0.0)
    )

    # Helper para convertir a lista de dicts con redondeos
    def df_to_records_rounded(df_local, round_map=None, top: int = None):
        if top is not None:
            df_local = df_local.head(top)
        records = df_local.to_dict(orient="records")
        if round_map:
            for r in records:
                for col, nd in round_map.items():
                    if col in r and isinstance(r[col], (int, float, np.floating)):
                        r[col] = round(float(r[col]), nd)
        return records

    round_item_map = {
        "total_qty": 4,
        "total_sales": 4,
        "total_gm": 4,
        "avg_gm_pct": 6
    }

    TOP_ITEMS_N = 5
    TOP_VENDORS_N = 10
    TOP_BRAND_N = 5
    TOP_PG_N = 5

    # ---------------------------
    # 3) TOP ITEMS (top 5 siempre)
    # ---------------------------
    # por volumen
    top_items_by_volume = item_group.sort_values(
        "total_qty", ascending=False
    )
    top_items_by_volume = df_to_records_rounded(
        top_items_by_volume, round_item_map, top=TOP_ITEMS_N
    )

    # por importe
    top_items_by_amount = item_group.sort_values(
        "total_sales", ascending=False
    )
    top_items_by_amount = df_to_records_rounded(
        top_items_by_amount, round_item_map, top=TOP_ITEMS_N
    )

    # por margen en $
    top_items_by_margin_amount = item_group.sort_values(
        "total_gm", ascending=False
    )
    top_items_by_margin_amount = df_to_records_rounded(
        top_items_by_margin_amount, round_item_map, top=TOP_ITEMS_N
    )

    # por margen %
    filtered_for_pct = item_group[item_group["total_sales"] > 0].copy()
    top_items_by_margin_pct = filtered_for_pct.sort_values(
        "avg_gm_pct", ascending=False
    )
    top_items_by_margin_pct = df_to_records_rounded(
        top_items_by_margin_pct, round_item_map, top=TOP_ITEMS_N
    )

    top_items = {
        "by_volume": top_items_by_volume,
        "by_amount": top_items_by_amount,
        "by_margin_amount": top_items_by_margin_amount,
        "by_margin_pct": top_items_by_margin_pct
    }

    # ---------------------------
    # 4) PROBLEMATIC ITEMS (< 15% GM%, ordenado ascendente)
    # ---------------------------
    problematic_items_df = item_group[
        (item_group["avg_gm_pct"] < 0.15) & (item_group["total_sales"] > 0)
    ].sort_values("avg_gm_pct", ascending=True)

    problematic_items = df_to_records_rounded(
        problematic_items_df, round_item_map
    )

    # ---------------------------
    # 5) VENDOR SUMMARY (Top 10 por total_gm desc)
    # ---------------------------
    vendor_summary = []
    if "selected_vendor" in df.columns:
        vendor_group = df.groupby("selected_vendor", dropna=False).agg(
            total_items=("item", "nunique"),
            total_lines=("item", "size"),
            total_sales=("line_sales", "sum"),
            total_gm=("line_gm", "sum"),
            avg_gm_pct=("gross_margin_pct", "mean")
        ).reset_index()

        vendor_group = vendor_group.sort_values("total_gm", ascending=False)

        vendor_round_map = {
            "total_sales": 4,
            "total_gm": 4,
            "avg_gm_pct": 6
        }

        vendor_summary = df_to_records_rounded(
            vendor_group, vendor_round_map, top=TOP_VENDORS_N
        )

    # ---------------------------
    # 6) DISTRIBUTION (brand / product_group)
    #    Top 5 por avg_gm_pct desc en cada caso
    # ---------------------------
    brand_distribution = []
    product_group_distribution = []

    dist_round_map = {
        "total_sales": 4,
        "total_gm": 4,
        "avg_gm_pct": 6
    }

    if "brand" in df.columns:
        brand_group = df.groupby("brand", dropna=False).agg(
            total_items=("item", "nunique"),
            total_lines=("item", "size"),
            total_sales=("line_sales", "sum"),
            total_gm=("line_gm", "sum"),
            avg_gm_pct=("gross_margin_pct", "mean")
        ).reset_index()

        brand_group = brand_group.sort_values("avg_gm_pct", ascending=False)

        brand_distribution = df_to_records_rounded(
            brand_group, dist_round_map, top=TOP_BRAND_N
        )

    if "product_group" in df.columns:
        pg_group = df.groupby("product_group", dropna=False).agg(
            total_items=("item", "nunique"),
            total_lines=("item", "size"),
            total_sales=("line_sales", "sum"),
            total_gm=("line_gm", "sum"),
            avg_gm_pct=("gross_margin_pct", "mean")
        ).reset_index()

        pg_group = pg_group.sort_values("avg_gm_pct", ascending=False)

        product_group_distribution = df_to_records_rounded(
            pg_group, dist_round_map, top=TOP_PG_N
        )

    # ---------------------------
    # 7) OUTPUT FINAL
    # ---------------------------
    output = {
        "general_summary": general_summary,
        "top_items": top_items,
        "problematic_items": problematic_items,
        "vendor_summary": vendor_summary,
        "distribution": {
            "by_brand": brand_distribution,
            "by_product_group": product_group_distribution
        }
    }

    return output
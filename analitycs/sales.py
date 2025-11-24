import pandas as pd
import numpy as np
import json

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

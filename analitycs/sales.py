import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from utils.envelope import build_tool_response

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
    top_n = 10
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

    # -----------------------------
    # 6) KPI POR SUBSIDIARY
    # -----------------------------
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
    # 7) INCOTERMS POR CLIENTE
    # -----------------------------
    incoterms_block = {"by_customer": []}
    if "incoterms" in df.columns:
        df_inc = df.copy()
        df_inc["incoterm_clean"] = df_inc["incoterms"].fillna("None")

        # Agrupar por cliente + incoterm
        inc_grp = (
            df_inc
            .groupby(["customer", "incoterm_clean"], dropna=False)
            .agg(
                order_count=("so_number", "nunique"),
                amount=("net_usd", "sum")
            )
            .reset_index()
        )

        # Armar estructura por cliente
        by_customer = []
        for customer, sub in inc_grp.groupby("customer"):
            details = [
                {
                    "incoterm": row["incoterm_clean"],
                    "order_count": int(row["order_count"]),
                    "amount": float(row["amount"])
                }
                for _, row in sub.iterrows()
            ]

            by_customer.append({
                "customer": customer,
                "incoterms_count": len(sub),
                "incoterms_detail": details
            })

        incoterms_block["by_customer"] = by_customer

    # -----------------------------
    # 8) DATA SAMPLE (primer y último registro)
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


    # -----------------------------
    # 9) ARMAR JSON FINAL
    # -----------------------------
    output = {
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
        "kpi_by_subsidiary": kpi_by_subsidiary,
        "incoterms": incoterms_block,
        "full_data_reference": "dataset_reference"
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
            "overview": summary,
            "distribution": {
                "inside_sales": dist_inside,
                "status": dist_status
            },
            "overdue_in_progress": overdue_list,
            "low_performance_indicators": low_performance_indicators,
            "full_data_reference": "dataset_reference"
    }

    return output

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

    top_brands_by_count = []
    if "brand" in df.columns:
        brand_count_group = (
            df.groupby("brand", dropna=False, as_index=False)
            .agg(
                appearance_count=("brand", "size"),
                unique_items=("item", "nunique"),
                unique_orders=("quote", "nunique"),
            )
            .sort_values(["appearance_count", "unique_orders"], ascending=False)
        )

        top_brands_by_count = df_to_records_rounded(
            brand_count_group,
            top=TOP_BRAND_N,
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
        "overview": general_summary,
        "top_items": top_items,
        "problematic_items": problematic_items,
        "vendor_summary": vendor_summary,
        "distribution": {
            "by_brand": brand_distribution,
            "by_product_group": product_group_distribution
        },
        "top_brands_by_count": top_brands_by_count,
    }

    return output

def general_summary_is_q_so(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate a general summary from IS quotes or sales orders DataFrame."""
    df["CreateDate"] = pd.to_datetime(df["CreateDate"])
    general_total = {
        "total_amount": float(df["Amount"].sum()),
        "total_transactions": int(df["QuoteNumber"].nunique() if "QuoteNumber" in df else df["SO"].nunique()),
        "total_customers": int(df["Customer"].nunique()),
        "start_date": str(df["CreateDate"].min().date()),
        "end_date": str(df["CreateDate"].max().date()),
    }
    
    df["period"] = df["CreateDate"].dt.to_period("M").astype(str)

    period_summary = (
        df.groupby("period", as_index=False)
        .agg(
            total_amount=("Amount", "sum"),
            total_transactions=("QuoteNumber", "nunique") if "QuoteNumber" in df else ("SO", "nunique"),
            total_customers=("Customer", "nunique"),
        )
    )

    period_summary["avg_ticket"] = (
        period_summary["total_amount"] / period_summary["total_transactions"]
    )

    customers_by_period = (
        df.groupby("period", as_index=False)
        .agg(
            customers=("Customer", "nunique"),
        )
    )
    customer_summary = {
        "total_unique_customers": df["Customer"].nunique(),
        "customers_by_period": customers_by_period.to_dict(orient="records")
    }

    timeline_general = (
        df.groupby("CreateDate", as_index=False)
        .agg(
            total_amount=("Amount", "sum"),
            total_transactions=("QuoteNumber", "nunique") if "QuoteNumber" in df else ("SO", "nunique")
        )
    )

    timeline_general["CreateDate"] = timeline_general["CreateDate"].dt.strftime("%Y-%m-%d")

    general_summary = {
        "general_total": general_total,
        "period_summary": period_summary.to_dict(orient="records"),
        "customer_summary": customer_summary,
        "timeline_general": timeline_general.to_dict(orient="records"),
    }
    return general_summary

    
def summarize_is_quotes(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summaries from the IS quotes DataFrame."""
    # 01. KPI by Inside Sale
    df = df.copy()

    # Asegurar tipos numéricos para evitar problemas
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    if "GrossMargin" in df.columns:
        df["GrossMargin"] = pd.to_numeric(df["GrossMargin"], errors="coerce")
    if "GrossMarginPct" in df.columns:
        df["GrossMarginPct"] = pd.to_numeric(df["GrossMarginPct"], errors="coerce")

    # 01. KPI by Inside Sale
    kpi_by_inside = (
        df.groupby("InsideSale", as_index=False)
        .agg(
            total_amount=("Amount", "sum"),
            num_quotes=("QuoteNumber", "nunique"),
        )
    )

    kpi_by_inside["avg_quote_amount"] = (
        kpi_by_inside["total_amount"] / kpi_by_inside["num_quotes"]
    )

    kpi_by_inside["rank_by_amount"] = (
        kpi_by_inside["total_amount"].rank(method="dense", ascending=False).astype(int)
    )

    kpi_by_inside = kpi_by_inside.sort_values("rank_by_amount").to_dict("records")
    

    # 02. Funnel + Win Rate by Inside Sale
    status_by_inside = (
        df.groupby(["InsideSale", "Status"], as_index=False)
        .agg(
            num_quotes=("QuoteNumber", "nunique"),
            total_amount=("Amount", "sum"),
            quote_list=("QuoteNumber", lambda x: list(x)),  # lista de quotes por status
        )
    )

    # Cálculo robusto de winrate (corrigiendo closed_amount)
    df_win = df.copy()
    df_win["is_closed"] = df_win["Status"].eq("Closed")
    df_win["closed_amount"] = np.where(df_win["is_closed"], df_win["Amount"], 0.0)

    winrate = (
        df_win.groupby("InsideSale", as_index=False)
        .agg(
            total_quotes=("QuoteNumber", "nunique"),
            total_amount=("Amount", "sum"),
            closed_quotes=("is_closed", "sum"),
            closed_amount=("closed_amount", "sum"),
        )
    )

    # Evitar división por cero
    winrate["win_rate_quotes"] = np.where(
        winrate["total_quotes"] > 0,
        winrate["closed_quotes"] / winrate["total_quotes"],
        np.nan,
    )
    winrate["win_rate_amount"] = np.where(
        winrate["total_amount"] > 0,
        winrate["closed_amount"] / winrate["total_amount"],
        np.nan,
    )

    status_summary_by_inside = []

    for inside, group in status_by_inside.groupby("InsideSale"):
        status_summary = {}
        for _, row in group.iterrows():
            status = row["Status"]
            status_summary[status] = {
                "num_quotes": int(row["num_quotes"]),
                "total_amount": float(row["total_amount"]),
                "quote_list": list(row["quote_list"]),
            }
        status_summary_by_inside.append({
            "inside_sale": inside,
            "status_summary": status_summary
        })

    # 03. Incoterms distribution
    incoterms_by_inside = (
        df.groupby(["InsideSale", "IncoTerms"], as_index=False)
        .agg(
            num_quotes=("QuoteNumber", "nunique"),
            total_amount=("Amount", "sum")
        )
    )
    incoterms_by_inside["amount_share_inside"] = (
        incoterms_by_inside
        .groupby("InsideSale")["total_amount"]
        .transform(lambda x: x / x.sum())
    )
    incoterms_payload = []

    for inside, group in incoterms_by_inside.groupby("InsideSale"):
        incoterms_payload.append({
            "inside_sale": inside,
            "incoterms": [
                {
                    "incoterm": row["IncoTerms"],
                    "num_quotes": int(row["num_quotes"]),
                    "total_amount": float(row["total_amount"]),
                    "amount_share_inside": float(row["amount_share_inside"]),
                }
                for _, row in group.iterrows()
            ],
        })

    # 04. NUEVO: Inside Sales con total cotizado < 30000 USD
    totals_by_inside = (
        df.groupby("InsideSale", as_index=False)
        .agg(total_amount=("Amount", "sum"))
    )

    under_30000 = totals_by_inside[totals_by_inside["total_amount"] < 30000]

    inside_sales_under_30000 = [
        {
            "inside_sale": row["InsideSale"],
            "total_amount": float(row["total_amount"]),
        }
        for _, row in under_30000.iterrows()
    ]

    # 05. NUEVO: Cotizaciones con margen < 20%
    quotes_under_20pct_margin = []
    if "GrossMarginPct" in df.columns:
        low_margin_df = df[df["GrossMarginPct"] < 0.20].copy()

        for _, row in low_margin_df.iterrows():
            quotes_under_20pct_margin.append({
                "quote_number": row["QuoteNumber"],
                "inside_sale": row["InsideSale"],
                "customer": row["Customer"],
                "amount": float(row["Amount"]) if pd.notna(row["Amount"]) else None,
                "gross_margin": float(row["GrossMargin"]) if "GrossMargin" in df.columns and pd.notna(row["GrossMargin"]) else None,
                "gross_margin_pct": float(row["GrossMarginPct"]) if pd.notna(row["GrossMarginPct"]) else None,
                "status": row["Status"],
            })

    # 06. NUEVO: Agrupación por Subsidiary con distribución por InsideSale
    subsidiary_distribution = []
    if {"Subsidiary", "InsideSale", "QuoteNumber", "Amount"}.issubset(df.columns):
        subsidiary_base = (
            df.groupby("Subsidiary", as_index=False)
            .agg(
                total_amount=("Amount", "sum"),
                num_quotes=("QuoteNumber", "nunique"),
            )
        )

        inside_by_subsidiary = (
            df.groupby(["Subsidiary", "InsideSale"], as_index=False)
            .agg(
                total_amount=("Amount", "sum"),
                num_quotes=("QuoteNumber", "nunique"),
            )
        )

        for _, sub_row in subsidiary_base.iterrows():
            subsidiary = sub_row["Subsidiary"]
            sub_total_amount = float(sub_row["total_amount"])
            sub_num_quotes = int(sub_row["num_quotes"])

            sub_inside = inside_by_subsidiary[
                inside_by_subsidiary["Subsidiary"] == subsidiary
            ].copy()
            sub_inside = sub_inside.sort_values("total_amount", ascending=False)

            inside_distribution = []
            for _, inside_row in sub_inside.iterrows():
                inside_total_amount = float(inside_row["total_amount"])
                inside_num_quotes = int(inside_row["num_quotes"])
                inside_distribution.append({
                    "inside_sale": inside_row["InsideSale"],
                    "total_amount": inside_total_amount,
                    "num_quotes": inside_num_quotes,
                    "amount_share_subsidiary": (
                        inside_total_amount / sub_total_amount
                        if sub_total_amount > 0
                        else 0.0
                    ),
                    "quotes_share_subsidiary": (
                        inside_num_quotes / sub_num_quotes
                        if sub_num_quotes > 0
                        else 0.0
                    ),
                })

            subsidiary_distribution.append({
                "subsidiary": subsidiary,
                "total_amount": sub_total_amount,
                "num_quotes": sub_num_quotes,
                "inside_sale_distribution": inside_distribution,
            })

    # General summary (tu función existente)
    general_summary = general_summary_is_q_so(df)

    return {
        "overview": general_summary,
        "kpi_by_inside": kpi_by_inside,
        "status_summary_by_inside": status_summary_by_inside,
        "incoterms_by_inside": incoterms_payload,
        "inside_sales_under_30000": inside_sales_under_30000,
        "quotes_under_20pct_margin": quotes_under_20pct_margin,
        "subsidiary_distribution": subsidiary_distribution,
        "full_data_reference": None,
    }
    
def summarize_items_quoted(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summaries from the items quoted DataFrame."""
    # Add calculated column for line value
    df["line_value"] = df["qty"] * df["unit_price"]
    # 01. More Used Vendor Summary
    vendor_summary = (
        df.groupby("selected_vendor", dropna=False, as_index=False)
        .agg(
            num_quotes=("quote", "nunique"),
            num_lines=("item", "count"),
            total_qty=("qty", "sum"),
            num_customers=("customer", "nunique"),
            num_brands=("brand", "nunique"),
            num_product_groups=("product_group", "nunique"),
            total_value=("line_value", "sum"),  # opcional
            quotes_list=("quote", lambda x: sorted(x.unique())),
            brands_list=("brand", lambda x: sorted(x.dropna().unique())),
        )
        .sort_values(["num_quotes", "num_lines"], ascending=False)
        .to_dict("records")
    )

    # 02. More demanded Brand Summary
    brand_summary = (
        df.groupby("brand", dropna=False, as_index=False)
        .agg(
            num_quotes=("quote", "nunique"),
            num_lines=("item", "count"),
            total_qty=("qty", "sum"),
            num_customers=("customer", "nunique"),
            num_vendors=("selected_vendor", "nunique"),
            total_value=("line_value", "sum"),
            quotes_list=("quote", lambda x: sorted(x.unique())),
        )
        .sort_values(["num_lines", "total_qty"], ascending=False)
        .to_dict("records")
    )

    top_brands_by_count = (
        df.groupby("brand", dropna=False, as_index=False)
        .agg(
            appearance_count=("brand", "size"),
            unique_quotes=("quote", "nunique"),
            unique_customers=("customer", "nunique"),
        )
        .sort_values(["appearance_count", "unique_quotes"], ascending=False)
        .head(10)
        .to_dict("records")
    )

    # 03. Customer By brand
    customer_brand_df = (
        df.groupby(["customer", "brand"], dropna=False, as_index=False)
        .agg(
            num_quotes=("quote", "nunique"),
            num_lines=("item", "count"),
            total_qty=("qty", "sum"),
            num_vendors=("selected_vendor", "nunique"),
            total_value=("line_value", "sum"),
        )
    )
    customer_brand = (
        customer_brand_df
        .sort_values(["customer", "total_qty"], ascending=[True, False])
        .groupby("customer", as_index=False)
        .apply(
            lambda g: pd.Series({
                "brands": [
                    {
                        "brand": row["brand"],
                        "num_quotes": int(row["num_quotes"]),
                        "num_lines": int(row["num_lines"]),
                        "total_qty": float(row["total_qty"]),
                        "num_vendors": int(row["num_vendors"]),
                        "total_value": float(row["total_value"]),
                    }
                    for _, row in g.iterrows()
                ]
            }),
            include_groups=False
        )
        .to_dict("records")
    )

    # 04. Summary by Inside Sales 
    inside_sales_summary = (
        df.groupby("inside_sales", dropna=False, as_index=False)
        .agg(
            num_product_groups=("product_group", "nunique"),
            product_groups_list=("product_group", lambda x: sorted({pg for pg in x if pd.notna(pg)})),

            num_brands=("brand", "nunique"),
            brands_list=("brand", lambda x: sorted({b for b in x if pd.notna(b)})),

            num_vendors=("selected_vendor", "nunique"),
            vendors_list=("selected_vendor", lambda x: sorted({v for v in x if pd.notna(v)})),
        )
        .sort_values(["num_product_groups", "num_brands", "num_vendors"], ascending=False)
        .head(10)
        .to_dict("records")
    )

    # 05. Top items quoted
    top_items_summary = (
        df.groupby(["item", "brand", "product_group"], dropna=False, as_index=False)
        .agg(
            num_lines=("quote", "count"),       # cuántas veces fue cotizado
            total_qty=("qty", "sum"),
            avg_price=("unit_price", "mean"),
            total_value=("line_value", "sum"),
        )
        .sort_values("total_value", ascending=False)
        .head(10)
        .to_dict("records")
    )
    
    return {
        "vendor_summary": vendor_summary,
        "brand_summary": brand_summary,
        "top_brands_by_count": top_brands_by_count,
        "inside_sales_summary": inside_sales_summary,
        "customer_brand": customer_brand,
        "top_items_summary": top_items_summary,
        "full_data_reference": None
    }
    
def analize_hr_desviado(df_cus_brand: pd.DataFrame, df_country_brand: pd.DataFrame) -> Dict[str, Any]:
    """
    Divide el dataframe por año y ordena cada subset por probabilidad descendente.
    
    Returns:
        df_2025, df_2024
    """
    
    df_customer_2025 = (
        df_cus_brand[df_cus_brand["year"] == 2025]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_customer_2024 = (
        df_cus_brand[df_cus_brand["year"] == 2024]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_country_2025 = (
        df_country_brand[df_country_brand["year"] == 2025]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_country_2024 = (
        df_country_brand[df_country_brand["year"] == 2024]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    result = {
        "vendors_by_customer_brand": {
            "current": {
                "label": "Based on current real time data",
                "data": df_customer_2025 if len(df_customer_2025) > 0 else "No recent data available this client" 
            },
            "old": {
                "label": "Based on data before to 2025",
                "data": df_customer_2024 if len(df_customer_2024) > 0 else "No historical data available this client"
            }
        },
        "vendors_by_country_brand": {
            "current": {
                "label": "Based on current real time data",
                "data": df_country_2025 if len(df_country_2025) > 0 else "No recent data available this country"
            },
            "old": {
                "label": "Based on data before to 2025",
                "data": df_country_2024 if len(df_country_2024) > 0 else "No historical data available this country"
            }
        }
    }

    return result

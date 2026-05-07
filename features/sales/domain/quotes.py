import pandas as pd
from typing import Dict, Any
import numpy as np

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

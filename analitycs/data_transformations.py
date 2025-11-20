import pandas as pd
from typing import Dict, List, Optional, Any

def tuple_to_dataframe(columns: List[str], rows: List[tuple]) -> pd.DataFrame:
    """Convert query result tuples to a pandas DataFrame."""
    return pd.DataFrame(rows, columns=columns)

def map_rows_to_dicts(columns: List[str], rows: List[tuple]) -> List[Dict[str, Any]]:
    """Map rows (tuples) to dicts using column names."""
    results: List[Dict[str, Any]] = []
    for row in rows:
        if columns:
            results.append({col: val for col, val in zip(columns, row)})
        else:
            # If no column names provided, return tuple under 'row'
            results.append({"row": row})
    return results

# Transformations functions for sales orders
def summarize_bookings_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate various summaries from the sales data DataFrame."""

    # 01. General Resume of the data
    general_resume = pd.Series({
        "records": len(df),
        "subsidiaries": df["subsidiary"].nunique(),
        "clients": df["customer"].nunique(),
        "gross_usd_total": df["gross_usd"].sum(),
        "net_usd_total": df["net_usd"].sum(),
        "gross_margin_total": df["gross_margin"].sum(),
        "gross_margin_pct_prom": df["gross_margin_pct"].mean(),
        "transactions": df["num_transactions"].sum()
    }).to_dict()

    # 02. KPI by period and subsidiary
    kpi_period_subsidiary = (
        df.groupby(["period", "subsidiary"], as_index=False)
          .agg(
              gross_usd=("gross_usd", "sum"),
              net_usd=("net_usd", "sum"),
              gross_margin=("gross_margin", "sum"),
              gm_pct_prom=("gross_margin_pct", "mean"),
              customers=("num_customers", "sum"),
              transactions=("num_transactions", "sum")
          )
    )
    kpi_period_subsidiary["gross_margin_pct_weighted"] = (
        kpi_period_subsidiary["gross_margin"] / kpi_period_subsidiary["gross_usd"]
    )
    kpi_period_subsidiary = kpi_period_subsidiary.to_dict("records")

    # 03. Top 10 customers by gross_usd
    top_customers = (
        df.groupby(["customer"], as_index=False)
        .agg(
            gross_usd_sum=("gross_usd", "sum"),
            gross_margin_sum=("gross_margin", "sum"),
            num_transactions_sum=("num_transactions", "sum")
        )
        .sort_values("gross_usd_sum", ascending=False)
        .head(10)
    ).to_dict("records")

    # 04. Margin distribution by customer
    cust_margin = (
        df.groupby("customer", as_index=False)
        .agg(
            gross_usd_sum=("gross_usd", "sum"),
            gross_margin_sum=("gross_margin", "sum")
        )
    )

    cust_margin["gross_margin_pct"] = (
        cust_margin["gross_margin_sum"] / cust_margin["gross_usd_sum"]
    )

    bins = [-1, 0, 0.10, 0.20, 1]
    labels = ["negative", "0-10%", "10-20%", "20%+"]

    cust_margin["margin_bucket"] = pd.cut(
        cust_margin["gross_margin_pct"], bins=bins, labels=labels
    )

    margin_bucket_summary = (
        cust_margin.groupby("margin_bucket", observed=True, as_index=False)
                .agg(
                    num_customers=("customer", "nunique"),
                    gross_usd_sum=("gross_usd_sum", "sum")
                )
    ).to_dict("records")

    return {
        "general_summary": general_resume,
        "kpi_period_subsidiary": kpi_period_subsidiary,
        "top_customers": top_customers,
        "margin_bucket_summary": margin_bucket_summary
    }

def summarize_customer_bookings(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summaries from the customer bookings DataFrame."""

    # 01. General Resume of the data
    general_summary = {
        "gross_usd_total": df["gross_usd"].sum(),
        "net_usd_total": df["net_usd"].sum(),
        "gross_margin_total": df["gross_margin"].sum(),
        "gross_margin_pct_weighted": df["gross_margin"].sum() / df["gross_usd"].sum(),
        "num_transactions": len(df),
        "avg_ticket": df["gross_usd"].mean(),
    }

    # 02. Timeline summary by period
    timeline_summary = (
        df.groupby("period", as_index=False)
        .agg(
            gross_usd_sum=("gross_usd", "sum"),
            gross_margin_sum=("gross_margin", "sum"),
            num_orders=("tranid", "count")
        )
    ).to_dict("records")

    # 03. Status summary
    status_summary = (
    df.groupby("status", as_index=False)
      .agg(
          num_orders=("tranid", "count"),
          gross_usd_sum=("gross_usd", "sum"),
          tranids=("tranid", lambda x: list(x))  # 👈 lista de tranids
      )
    ).to_dict("records")


    # 04. Outliers summary
    top_orders = (
        df.sort_values("gross_usd", ascending=False)
        .head(5)[["tranid", "date", "status", "gross_usd", "gross_margin", "gross_margin_pct"]]
        .to_dict(orient="records")
    )

    negative_margin = df[df["gross_margin"] < 0].to_dict(orient="records")
    
    outliers_summary = {
        "top_orders": top_orders,
        "negative_margin_orders": negative_margin
    }
    
    
    return {
        "general_summary": general_summary,
        "timeline_summary": timeline_summary,
        "status_summary": status_summary,
        "outliers_summary": outliers_summary
    }

def summarize_is_bookings(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summaries from the IS bookings DataFrame."""

    # 01. KPI by Inside Sale
    kpi_by_inside = (
        df.groupby("InsideSale", as_index=False)
        .agg(
            total_amount=("Amount", "sum"),
            num_orders=("SO", "nunique"),
        )
    )

    kpi_by_inside["avg_ticket"] = (
        kpi_by_inside["total_amount"] / kpi_by_inside["num_orders"]
    )

    kpi_by_inside["rank_by_amount"] = (
        kpi_by_inside["total_amount"].rank(method="dense", ascending=False).astype(int)
    )

    kpi_by_inside = kpi_by_inside.sort_values("rank_by_amount").to_dict("records")

    # 02. Status by Inside Sale
    status_by_inside = (
        df.groupby(["InsideSale", "Status"], as_index=False)
        .agg(
            num_orders=("SO", "nunique"),
            total_amount=("Amount", "sum"),
            so_list=("SO", lambda x: list(x)),  # lista de SO por inside+status
        )
    )
    status_distribution_by_is = []

    for inside, group in status_by_inside.groupby("InsideSale"):
        status_summary = {}
        for _, row in group.iterrows():
            status = row["Status"]
            status_summary[status] = {
                "num_orders": int(row["num_orders"]),
                "total_amount": float(row["total_amount"]),
                "so_list": list(row["so_list"]),
            }

        status_distribution_by_is.append({
            "inside_sale": inside,
            "status_summary": status_summary,
        })

    # 03. Top Customers by amount
    top_customers_raw = (
        df.groupby(["InsideSale", "Customer"], as_index=False)
        .agg(
            numero_ordenes=("SO", "nunique"),
            amount=("Amount", "sum")
        )
    )
    top_customers_raw = top_customers_raw.sort_values(
        ["InsideSale", "amount"], ascending=[True, False]
    )
    TOP_N_CUSTOMERS = 3
    top5_customers_per_inside = (
        top_customers_raw
        .groupby("InsideSale")
        .head(TOP_N_CUSTOMERS)
    )
    inside_top5_amount = (
        top5_customers_per_inside
        .groupby("InsideSale", as_index=False)
        .agg(
            top5_total_amount=("amount", "sum")
        )
    )
    TOP_N_INSIDES = 5

    top5_insides_by_top5_amount = (
        inside_top5_amount
        .sort_values("top5_total_amount", ascending=False)
        .head(TOP_N_INSIDES)
    )

    insides_keep = top5_insides_by_top5_amount["InsideSale"]
    top5_customers_top5_insides = top5_customers_per_inside[
        top5_customers_per_inside["InsideSale"].isin(insides_keep)
    ]
    result = []

    for inside, group in top5_customers_top5_insides.groupby("InsideSale"):
        total_top5_amount = float(group["amount"].sum())
        result.append({
            "inside_sale": inside,
            "top5_total_amount": total_top5_amount,
            "top_customers": [
                {
                    "customer": row["Customer"],
                    "numero_ordenes": int(row["numero_ordenes"]),
                    "amount": float(row["amount"]),
                }
                for _, row in group.iterrows()
            ],
        })
    top5_insides_by_customer = sorted(result, key=lambda x: x["top5_total_amount"], reverse=True)
    
    general_summary = general_summary_is_q_so(df)

    return {
        "general_summary": general_summary,
        "kpi_by_inside": kpi_by_inside,
        "status_by_inside": status_distribution_by_is,
        "top_customers": top5_insides_by_customer
    }

def summarize_is_quotes(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate summaries from the IS quotes DataFrame."""
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

    winrate = (
        df.assign(
            is_closed=df["Status"].eq("Closed")
        )
        .groupby("InsideSale", as_index=False)
        .agg(
            total_quotes=("QuoteNumber", "nunique"),
            total_amount=("Amount", "sum"),
            closed_quotes=("is_closed", "sum"),
            closed_amount=("Amount", lambda x: x[df["Status"].eq("Closed")].sum())
        )
    )

    winrate["win_rate_quotes"] = winrate["closed_quotes"] / winrate["total_quotes"]
    winrate["win_rate_amount"] = winrate["closed_amount"] / winrate["total_amount"]

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

    # Incoterms distribution
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

    general_summary = general_summary_is_q_so(df)

    return {
        "general_summary": general_summary,
        "kpi_by_inside": kpi_by_inside,
        "status_summary_by_inside": status_summary_by_inside,
        "incoterms_by_inside": incoterms_payload
    }

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
        "inside_sales_summary": inside_sales_summary,
        "customer_brand": customer_brand,
        "top_items_summary": top_items_summary
    }

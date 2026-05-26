import pandas as pd
import numpy as np
from typing import Dict, Any

def summarize_sold_items(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Espera un DataFrame con al menos las columnas:
    ['customer', 'quote', 'status', 'date', 'inside_sales',
     'item', 'item_description', 'brand', 'product_group',
     'selected_vendor', 'qty', 'unit_price', 'unit_cost',
     'gross_margin_pct', 'inbound_freight_cost',
     'outbound_freight_cost']

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

    numeric_defaults = {
        "qty": 0.0,
        "unit_price": 0.0,
        "unit_cost": np.nan,
        "gross_margin_pct": np.nan,
        "inbound_freight_cost": 0.0,
        "outbound_freight_cost": 0.0,
    }
    for col, default_value in numeric_defaults.items():
        if col not in df.columns:
            df[col] = default_value
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ventas de línea
    df["line_sales"] = df["qty"] * df["unit_price"]

    # Costo de línea: prioridad unit_cost, luego gross_margin_pct
    has_unit_cost = df["unit_cost"].notna()

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
    df["line_inbound_freight_cost"] = df["inbound_freight_cost"].fillna(0.0)
    df["line_outbound_freight_cost"] = df["outbound_freight_cost"].fillna(0.0)
    df["line_gm"] = (
        df["line_sales"]
        - df["line_cost"]
        - df["line_inbound_freight_cost"]
        - df["line_outbound_freight_cost"]
    )
    df["line_gm"] = df["line_gm"].fillna(0.0)
    df["line_sales"] = df["line_sales"].fillna(0.0)
    df["line_cost"] = df["line_cost"].fillna(0.0)

    # ---------------------------
    # 1) GENERAL SUMMARY
    # ---------------------------
    total_qty = float(df["qty"].sum())
    total_sales = float(df["line_sales"].sum())
    total_cost = float(df["line_cost"].sum())
    total_inbound_freight_cost = float(df["line_inbound_freight_cost"].sum())
    total_outbound_freight_cost = float(df["line_outbound_freight_cost"].sum())
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
        "total_cost": round(total_cost, 4),
        "total_inbound_freight_cost": round(total_inbound_freight_cost, 4),
        "total_outbound_freight_cost": round(total_outbound_freight_cost, 4),
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
        total_cost=("line_cost", "sum"),
        total_inbound_freight_cost=("line_inbound_freight_cost", "sum"),
        total_outbound_freight_cost=("line_outbound_freight_cost", "sum"),
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
        "total_cost": 4,
        "total_inbound_freight_cost": 4,
        "total_outbound_freight_cost": 4,
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
            total_cost=("line_cost", "sum"),
            total_inbound_freight_cost=("line_inbound_freight_cost", "sum"),
            total_outbound_freight_cost=("line_outbound_freight_cost", "sum"),
            total_gm=("line_gm", "sum"),
        ).reset_index()

        vendor_group["avg_gm_pct"] = np.where(
            vendor_group["total_sales"] > 0,
            vendor_group["total_gm"] / vendor_group["total_sales"],
            0.0
        )

        vendor_group = vendor_group.sort_values("total_gm", ascending=False)

        vendor_round_map = {
            "total_sales": 4,
            "total_cost": 4,
            "total_inbound_freight_cost": 4,
            "total_outbound_freight_cost": 4,
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
        "total_cost": 4,
        "total_inbound_freight_cost": 4,
        "total_outbound_freight_cost": 4,
        "total_gm": 4,
        "avg_gm_pct": 6
    }

    if "brand" in df.columns:
        brand_group = df.groupby("brand", dropna=False).agg(
            total_items=("item", "nunique"),
            total_lines=("item", "size"),
            total_sales=("line_sales", "sum"),
            total_cost=("line_cost", "sum"),
            total_inbound_freight_cost=("line_inbound_freight_cost", "sum"),
            total_outbound_freight_cost=("line_outbound_freight_cost", "sum"),
            total_gm=("line_gm", "sum"),
        ).reset_index()

        brand_group["avg_gm_pct"] = np.where(
            brand_group["total_sales"] > 0,
            brand_group["total_gm"] / brand_group["total_sales"],
            0.0
        )

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
            total_cost=("line_cost", "sum"),
            total_inbound_freight_cost=("line_inbound_freight_cost", "sum"),
            total_outbound_freight_cost=("line_outbound_freight_cost", "sum"),
            total_gm=("line_gm", "sum"),
        ).reset_index()

        pg_group["avg_gm_pct"] = np.where(
            pg_group["total_sales"] > 0,
            pg_group["total_gm"] / pg_group["total_sales"],
            0.0
        )

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
    print("Items summary generated successfully.", output)
    return output

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

def sold_brands_recurrence_metrics(df: pd.DataFrame, top_n: int = 10) -> dict:
    """
    Analiza recurrencia de marcas, priorizando frecuencia de aparición.
    
    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame con al menos estas columnas:
        ['quote', 'brand', 'item', 'customer', 'qty', 'unit_price', 'unit_cost']
    top_n : int, default 10
        Número de marcas a devolver en rankings principales.
    
    Retorna
    -------
    dict con:
        - brand_recurrence: tabla principal enfocada en recurrencia
        - top_recurrent_brands: top marcas por recurrencia
        - brand_amount_ranking: sección secundaria, marcas por monto
        - summary_kpis: KPIs generales
    """
    
    data = df.copy()

    # Normalización básica
    data["brand"] = data["brand"].astype(str).str.strip()
    data["quote"] = data["quote"].astype(str).str.strip()
    data["item"] = data["item"].astype(str).str.strip()
    data["customer"] = data["customer"].astype(str).str.strip()

    numeric_cols = ["qty", "unit_price", "unit_cost"]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)

    # Métricas monetarias
    data["revenue"] = data["qty"] * data["unit_price"]
    data["cost"] = data["qty"] * data["unit_cost"]
    data["gross_profit"] = data["revenue"] - data["cost"]

    total_lines = len(data)
    total_quotes = data["quote"].nunique()
    total_revenue = data["revenue"].sum()

    # Tabla principal: recurrencia de marcas
    brand_recurrence = (
        data.groupby("brand", dropna=False)
        .agg(
            recurrence_lines=("brand", "size"),       # veces que aparece
            quotes_count=("quote", "nunique"),        # en cuántas quotes aparece
            items_count=("item", "count"),            # cuántas líneas/items abarca
            unique_items=("item", "nunique"),         # cuántos productos distintos
            customers_count=("customer", "nunique"),  # en cuántos clientes aparece
            total_qty=("qty", "sum"),
            total_revenue=("revenue", "sum"),
            total_cost=("cost", "sum"),
            gross_profit=("gross_profit", "sum"),
            avg_unit_price=("unit_price", "mean"),
            avg_unit_cost=("unit_cost", "mean"),
        )
        .reset_index()
    )

    # Indicadores derivados
    brand_recurrence["recurrence_pct_lines"] = np.where(
        total_lines > 0,
        brand_recurrence["recurrence_lines"] / total_lines,
        0
    )

    brand_recurrence["quotes_pct"] = np.where(
        total_quotes > 0,
        brand_recurrence["quotes_count"] / total_quotes,
        0
    )

    brand_recurrence["revenue_share"] = np.where(
        total_revenue > 0,
        brand_recurrence["total_revenue"] / total_revenue,
        0
    )

    brand_recurrence["margin_pct"] = np.where(
        brand_recurrence["total_revenue"] != 0,
        brand_recurrence["gross_profit"] / brand_recurrence["total_revenue"],
        0
    )

    brand_recurrence["avg_revenue_per_occurrence"] = np.where(
        brand_recurrence["recurrence_lines"] > 0,
        brand_recurrence["total_revenue"] / brand_recurrence["recurrence_lines"],
        0
    )

    brand_recurrence["avg_revenue_per_quote"] = np.where(
        brand_recurrence["quotes_count"] > 0,
        brand_recurrence["total_revenue"] / brand_recurrence["quotes_count"],
        0
    )

    # Ranking principal: recurrencia
    brand_recurrence = brand_recurrence.sort_values(
        by=["recurrence_lines", "quotes_count", "total_revenue"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    # Top de marcas recurrentes
    top_recurrent_brands = brand_recurrence.head(top_n).copy()

    # Sección secundaria: marcas por monto
    brand_amount_ranking = (
        brand_recurrence
        .sort_values(
            by=["total_revenue", "gross_profit", "recurrence_lines"],
            ascending=[False, False, False]
        )
        .reset_index(drop=True)
        .head(top_n)
        .copy()
    )

    # KPIs generales
    summary_kpis = pd.DataFrame({
        "metric": [
            "total_lines",
            "total_quotes",
            "total_brands",
            "total_revenue",
            "top_brand_by_recurrence",
            "top_brand_recurrence_lines",
            "top_brand_recurrence_revenue",
            "top_brand_by_amount",
            "top_brand_amount_value"
        ],
        "value": [
            total_lines,
            total_quotes,
            brand_recurrence["brand"].nunique(),
            total_revenue,
            brand_recurrence.iloc[0]["brand"] if len(brand_recurrence) else None,
            brand_recurrence.iloc[0]["recurrence_lines"] if len(brand_recurrence) else None,
            brand_recurrence.iloc[0]["total_revenue"] if len(brand_recurrence) else None,
            brand_amount_ranking.iloc[0]["brand"] if len(brand_amount_ranking) else None,
            brand_amount_ranking.iloc[0]["total_revenue"] if len(brand_amount_ranking) else None,
        ]
    })

    return {
        "brand_recurrence": brand_recurrence.to_dict(orient="records"),
        "top_recurrent_brands": top_recurrent_brands.to_dict(orient="records"),
        "brand_amount_ranking": brand_amount_ranking.to_dict(orient="records"),
        "summary_kpis": summary_kpis.to_dict(orient="records"),
    }

def quoted_brands_recurrence_metrics(df: pd.DataFrame, top_n: int = 10) -> dict:
    """
    Analiza recurrencia de marcas en items cotizados.
    Prioriza frecuencia de aparición de la marca y, secundariamente,
    muestra ranking por monto cotizado.

    Parámetros
    ----------
    df : pd.DataFrame
        DataFrame con columnas esperadas:
        ['customer', 'quote', 'status', 'date', 'inside_sales',
         'item', 'brand', 'product_group', 'selected_vendor',
         'qty', 'unit_price']
    top_n : int
        Número de marcas a devolver en los rankings.

    Retorna
    -------
    dict con:
        - brand_recurrence: tabla principal enfocada en recurrencia
        - top_recurrent_brands: top marcas por recurrencia
        - brand_amount_ranking: ranking secundario por monto cotizado
        - summary_kpis: KPIs generales
    """

    data = df.copy()

    # Normalización
    text_cols = [
        "customer", "quote", "status", "inside_sales",
        "item", "brand", "product_group", "selected_vendor"
    ]
    for col in text_cols:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip()

    if "date" in data.columns:
        data["date"] = pd.to_datetime(data["date"], errors="coerce")

    for col in ["qty", "unit_price"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)

    # Métrica monetaria principal en cotizaciones
    data["quoted_amount"] = data["qty"] * data["unit_price"]

    total_lines = len(data)
    total_quotes = data["quote"].nunique()
    total_amount = data["quoted_amount"].sum()
    total_qty = data["qty"].sum()

    # Tabla principal: recurrencia de marca en cotizaciones
    brand_recurrence = (
        data.groupby("brand", dropna=False)
        .agg(
            recurrence_lines=("brand", "size"),         # veces que aparece
            quotes_count=("quote", "nunique"),         # en cuántas quotes aparece
            items_count=("item", "count"),             # líneas/items que abarca
            unique_items=("item", "nunique"),          # items distintos
            customers_count=("customer", "nunique"),   # clientes distintos
            inside_sales_count=("inside_sales", "nunique"),
            total_qty=("qty", "sum"),
            total_quoted_amount=("quoted_amount", "sum"),
            avg_unit_price=("unit_price", "mean"),
            min_unit_price=("unit_price", "min"),
            max_unit_price=("unit_price", "max"),
        )
        .reset_index()
    )

    # KPIs derivados
    brand_recurrence["recurrence_pct_lines"] = np.where(
        total_lines > 0,
        brand_recurrence["recurrence_lines"] / total_lines,
        0
    )

    brand_recurrence["quotes_pct"] = np.where(
        total_quotes > 0,
        brand_recurrence["quotes_count"] / total_quotes,
        0
    )

    brand_recurrence["qty_share"] = np.where(
        total_qty > 0,
        brand_recurrence["total_qty"] / total_qty,
        0
    )

    brand_recurrence["quoted_amount_share"] = np.where(
        total_amount > 0,
        brand_recurrence["total_quoted_amount"] / total_amount,
        0
    )

    brand_recurrence["avg_amount_per_occurrence"] = np.where(
        brand_recurrence["recurrence_lines"] > 0,
        brand_recurrence["total_quoted_amount"] / brand_recurrence["recurrence_lines"],
        0
    )

    brand_recurrence["avg_amount_per_quote"] = np.where(
        brand_recurrence["quotes_count"] > 0,
        brand_recurrence["total_quoted_amount"] / brand_recurrence["quotes_count"],
        0
    )

    brand_recurrence["avg_qty_per_occurrence"] = np.where(
        brand_recurrence["recurrence_lines"] > 0,
        brand_recurrence["total_qty"] / brand_recurrence["recurrence_lines"],
        0
    )

    # Orden principal: recurrencia
    brand_recurrence = brand_recurrence.sort_values(
        by=["recurrence_lines", "quotes_count", "total_quoted_amount"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    # Top marcas recurrentes
    top_recurrent_brands = brand_recurrence.head(top_n).copy()

    # Sección secundaria: ranking por monto cotizado
    brand_amount_ranking = (
        brand_recurrence
        .sort_values(
            by=["total_quoted_amount", "recurrence_lines", "quotes_count"],
            ascending=[False, False, False]
        )
        .reset_index(drop=True)
        .head(top_n)
        .copy()
    )

    # KPIs resumen
    summary_kpis = pd.DataFrame({
        "metric": [
            "total_quote_lines",
            "total_quotes",
            "total_brands",
            "total_quoted_qty",
            "total_quoted_amount",
            "top_brand_by_recurrence",
            "top_brand_recurrence_lines",
            "top_brand_recurrence_quotes",
            "top_brand_recurrence_amount",
            "top_brand_by_amount",
            "top_brand_amount_value"
        ],
        "value": [
            total_lines,
            total_quotes,
            brand_recurrence["brand"].nunique(),
            total_qty,
            total_amount,
            brand_recurrence.iloc[0]["brand"] if len(brand_recurrence) else None,
            brand_recurrence.iloc[0]["recurrence_lines"] if len(brand_recurrence) else None,
            brand_recurrence.iloc[0]["quotes_count"] if len(brand_recurrence) else None,
            brand_recurrence.iloc[0]["total_quoted_amount"] if len(brand_recurrence) else None,
            brand_amount_ranking.iloc[0]["brand"] if len(brand_amount_ranking) else None,
            brand_amount_ranking.iloc[0]["total_quoted_amount"] if len(brand_amount_ranking) else None,
        ]
    })

    return {
        "brand_recurrence": brand_recurrence.to_dict(orient="records"),
        "top_recurrent_brands": top_recurrent_brands.to_dict(orient="records"),
        "brand_amount_ranking": brand_amount_ranking.to_dict(orient="records"),
        "summary_kpis": summary_kpis.to_dict(orient="records")
    }

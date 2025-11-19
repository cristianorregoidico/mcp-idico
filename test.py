import pandas as pd
from typing import Dict, List, Optional, Any
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer, get_opportunities_by_inside
sql = get_bookings_by_period("'2025-11-01'", "'2025-11-18'", "NULL")
print("sql",sql)
conn = NetSuiteConnection()
with conn.managed() as ns:
    columns, rows = ns.execute_query(sql)

df = pd.DataFrame(rows, columns=columns)

resumen_general = pd.Series({
    "registros": len(df),
    "subsidiarias": df["subsidiary"].nunique(),
    "clientes": df["customer"].nunique(),
    "gross_usd_total": df["gross_usd"].sum(),
    "net_usd_total": df["net_usd"].sum(),
    "gross_margin_total": df["gross_margin"].sum(),
    "gross_margin_pct_prom": df["gross_margin_pct"].mean(),
    "transacciones": df["num_transactions"].sum()
})
print("\nResumen general:\n", resumen_general.to_dict())

resumen_por_subsidiaria = (
    df.groupby("subsidiary")
      .agg(
          registros=("customer", "size"),
          clientes_unicos=("customer", "nunique"),
          gross_usd=("gross_usd", "sum"),
          gross_margin=("gross_margin", "sum"),
          gm_pct_prom=("gross_margin_pct", "mean"),
          transacciones=("num_transactions", "sum")
      )
      .sort_values("gross_usd", ascending=False)
)
print("\nResumen por subsidiaria:\n", resumen_por_subsidiaria.reset_index().to_dict("records"))

resumen_por_periodo = (
    df.groupby("period")
      .agg(
          registros=("customer", "size"),
          gross_usd=("gross_usd", "sum"),
          net_usd=("net_usd", "sum"),
          gross_margin=("gross_margin", "sum"),
          gm_pct_prom=("gross_margin_pct", "mean")
      )
)
print("\nResumen por periodo:\n", resumen_por_periodo.reset_index().to_dict("records"))
top_customers = (
        df.nlargest(5, "gross_usd")[["period","subsidiary","customer","gross_usd","gross_margin","gross_margin_pct"]]
          .to_dict("records")
    )
print("\nTop 5 clientes por gross_usd:\n", top_customers)
print("\nDescribe numérico:\n", df.describe())
# Map rows (tuples) to dicts using column names
results: List[Dict[str, Any]] = []
for row in rows:
    if columns:
        results.append({col: val for col, val in zip(columns, row)})
    else:
        # If no column names provided, return tuple under 'row'
        results.append({"row": row})
        
#print("results",results)
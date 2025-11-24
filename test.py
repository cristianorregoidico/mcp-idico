import pandas as pd
from typing import Dict, List, Optional, Any
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer, get_opportunities_by_inside,get_bookings_by_customer
from analitycs.data_transformations import tuple_to_dataframe, summarize_bookings_data, summarize_is_bookings, summarize_is_quotes, summarize_items_quoted
from analitycs.sales import finance_summary
sql = get_bookings_by_customer("2025-11-17", "2025-11-21", "")
print("sql",sql)
conn = NetSuiteConnection()
with conn.managed() as ns:
    columns, rows = ns.execute_query(sql)
print("columns",columns)
print("rows",rows)
df = tuple_to_dataframe(columns, rows)
summary = finance_summary(df)
print("summary",summary)
# Map rows (tuples) to dicts using column names
results: List[Dict[str, Any]] = []
for row in rows:
    if columns:
        results.append({col: val for col, val in zip(columns, row)})
    else:
        # If no column names provided, return tuple under 'row'
        results.append({"row": row})
        
#print("results",results)
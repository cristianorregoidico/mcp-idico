import pandas as pd
from typing import Dict, List, Optional, Any
from connections.netsuite import NetSuiteConnection
from connections.netsuite_querys import get_quotes_by_inside, get_sales_orders_by_inside, get_bookings_by_period, get_items_quoted_by_customer,get_bookings_data, get_op_so_data, get_sold_items_by_period
from analitycs.data_transformations import tuple_to_dataframe, summarize_bookings_data, summarize_is_bookings, summarize_is_quotes, summarize_items_quoted
from analitycs.sales import finance_summary, opportunity_summary, analyze_inside_sales, summarize_sold_items
from utils.json_df import save_result_to_json, load_dataset_from_json
from connections.postgresql_querys import get_ob_time_delivery, get_customer_imports
from connections.postgresql import execute_pg_query_dev
from analitycs.operations import on_time_delivery_summary, build_imports_summary
sql = get_customer_imports("SQM INDUSTRIAL")
print("sql",sql)
columns, rows = execute_pg_query_dev(sql)
#conn = NetSuiteConnection()
# with conn.managed() as ns:
#     columns, rows = ns.execute_query(sql)
print("columns",columns)
print("rows",rows)

df = tuple_to_dataframe(columns, rows)
# so = df.to_dict(orient="records")
# print("so",so)
summary = build_imports_summary(df)
# summary["so_details"] = so
print("summary",summary)
# Map rows (tuples) to dicts using column names
# dataset_reference = save_result_to_json(columns, rows, "Full Bookings Dataset", name="bookings_data")
#print("dataset_reference",dataset_reference)

# df_from_json, description = load_dataset_from_json(dataset_reference)
# print("df_from_json",df_from_json)
results: List[Dict[str, Any]] = []
for row in rows:
    if columns:
        results.append({col: val for col, val in zip(columns, row)})
    else:
        # If no column names provided, return tuple under 'row'
        results.append({"row": row})
        
#print("results",results)
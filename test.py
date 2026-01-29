import pandas as pd
from typing import Dict, List, Optional, Any
from connections.netsuite import NetSuiteConnection
from analitycs.data_transformations import tuple_to_dataframe
from utils.json_df import save_result_to_json, load_dataset_from_json
from connections.postgresql_querys import get_helga_guides_query
from connections.netsuite_querys import get_quotes_by_inside
from connections.postgresql import execute_pg_query_dev, execute_pg_query
from analitycs.sales import summarize_is_quotes

sql = get_quotes_by_inside("2026-01-01", "2026-01-29", "ANDRES RIVERA", "")
print("sql",sql)
#columns, rows = execute_pg_query(sql)
conn = NetSuiteConnection()
with conn.managed() as ns:
    columns, rows = ns.execute_query(sql)
print("columns",columns)
print("rows",rows)

df = tuple_to_dataframe(columns, rows)
# so = df.to_dict(orient="records")
# print("so",so)
summary = summarize_is_quotes(df)
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
import pandas as pd
from typing import Dict, List, Any

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



from typing import Any, Dict, Optional, List
from connections.postgresql import execute_pg_query
from connections.postgresql_querys import pending_guides_query
from utils.json_df import save_result_to_json
from analitycs.data_transformations import tuple_to_dataframe


def helga_guides() -> Dict[str, Any]:
    """Retrieve the helga guides pending to be delivery.

    Args:
        No args needed
    Returns:
        Dict[str, Any]: Collection of helga guides.
    """
    
    sql = pending_guides_query()
    columns, rows = execute_pg_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"List of guides pending for delivery", name="guides_oneding_delivery")
    
    df = tuple_to_dataframe(columns, rows)
    results = df.to_dict(orient="records")
    print("results",results)
    return {
        "results": results,
        "full_data_reference": dataset_reference
    }

POSTGRES_TOOLS: List = [
    helga_guides
]
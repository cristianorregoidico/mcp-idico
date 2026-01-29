from typing import Any, Dict, Optional, List
from connections.postgresql import execute_pg_query, execute_pg_query_dev
from connections.postgresql_querys import get_helga_guides_query, get_on_time_delivery, get_customer_imports_data
from utils.json_df import save_result_to_json
from utils.date import get_month_start_and_today
from analitycs.operations import on_time_delivery_summary, build_imports_summary
from analitycs.data_transformations import tuple_to_dataframe

def get_helga_guides(po: Optional[str] = None, status: Optional[str] = None, service: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve helga guides based on po, status and service filters.
    
    Use this tool when user asks for status, po or service information about helga guides.

    Args:
        po: Optional[str] - Purchase Order number to filter by.
        status: Optional[str] - Status to filter by.
        service: Optional[str] - Service to filter by.
    Returns:
        Dict[str, Any]: Collection of helga guides.
    """
    
    sql = get_helga_guides_query(po=po, status=status, service=service)
    columns, rows = execute_pg_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"List of guides pending for delivery", name="guides_oneding_delivery")
    
    df = tuple_to_dataframe(columns, rows)
    results = df.to_dict(orient="records")
    print("results",results)
    return {
        "results": results,
        "full_data_reference": dataset_reference
    }
    
def get_otd_indicators(initial_date: Optional[str] = None, final_date: Optional[str] = None, so_number: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve the on time delivery indicators by period.
    
    Use this tool when user asks for on time delivery indicators by period or sales order number.

    Args:
        initial_date (str): Initial date in format 'YYYY-MM-DD'.
        final_date (str): Final date in format 'YYYY-MM-DD'.
        so_number (str, optional): Sales order number to filter. Defaults to None.
    Returns:
        Dict[str, Any]: On time delivery indicators.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    
    sql = get_on_time_delivery(start_q_date, final_q_date, so_number)
    columns, rows = execute_pg_query_dev(sql)
    
    dataset_reference = save_result_to_json(columns, rows, "The full items delivery by period", name="otd_data")
    
    df = tuple_to_dataframe(columns, rows)
    
    results = on_time_delivery_summary(df)
    results["full_data_reference"] = dataset_reference
    if so_number:
        so_details = df.to_dict(orient="records")
        results["so_details"] = so_details
    
    return results

    
def get_customer_imports(customer_name: str) -> Dict[str, Any]:
    """Retrieve the imports summary for a given customer.
    
    Use this tool when user asks for imports summary for a specific customer.

    Args:
        customer_name (str): Name of the customer.
    Returns:
        Dict[str, Any]: Imports summary.
    """
    sql = get_customer_imports_data(customer_name)
    columns, rows = execute_pg_query_dev(sql)
    
    df = tuple_to_dataframe(columns, rows)
    
    summary = build_imports_summary(df)
    
    return summary

OPS_TOOLS: List = [
    get_helga_guides,
    get_otd_indicators,
    get_customer_imports
]
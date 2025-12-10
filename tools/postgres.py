from typing import Any, Dict, Optional, List
from connections.postgresql import execute_pg_query, execute_pg_query_dev
from connections.postgresql_querys import pending_guides_query, get_ob_time_delivery, get_scorecard_by_is_month, get_scorecard_by_is_daily, get_scorecard_by_is_year
from utils.json_df import save_result_to_json
from utils.date import get_month_start_and_today
from analitycs.operations import on_time_delivery_summary
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

def get_otd_indicators(initial_date: Optional[str] = None, final_date: Optional[str] = None, so_number: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve the on time delivery indicators by period.

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
    
    sql = get_ob_time_delivery(start_q_date, final_q_date, so_number)
    columns, rows = execute_pg_query_dev(sql)
    
    dataset_reference = save_result_to_json(columns, rows, "The full items delivery by period", name="otd_data")
    
    df = tuple_to_dataframe(columns, rows)
    
    results = on_time_delivery_summary(df)
    results["full_data_reference"] = dataset_reference
    if so_number:
        so_details = df.to_dict(orient="records")
        results["so_details"] = so_details
    
    return results

def get_scorecard_by_is() -> Dict[str, Any]:
    """Retrieve the scorecard metrics by Inside Sales Daily, Monthly and Yearly.

    Args:
        No args needed
    Returns:
        Dict[str, Any]: Scorecard by IS.
    """
    sql_monthly = get_scorecard_by_is_month()
    columns_monthly, rows_monthly = execute_pg_query_dev(sql_monthly)
    df_monthly = tuple_to_dataframe(columns_monthly, rows_monthly)
    monthly_data = df_monthly.to_dict(orient="records")
    
    sql_daily = get_scorecard_by_is_daily()
    columns_daily, rows_daily = execute_pg_query_dev(sql_daily)
    df_daily = tuple_to_dataframe(columns_daily, rows_daily)
    daily_data = df_daily.to_dict(orient="records")
    
    sql_yearly = get_scorecard_by_is_year()
    columns_yearly, rows_yearly = execute_pg_query_dev(sql_yearly)
    df_yearly = tuple_to_dataframe(columns_yearly, rows_yearly)
    yearly_data = df_yearly.to_dict(orient="records")
    
    return {
        "monthly_scorecard": monthly_data,
        "daily_scorecard": daily_data,
        "yearly_scorecard": yearly_data
    }
    
POSTGRES_TOOLS: List = [
    helga_guides,
    get_otd_indicators,
    get_scorecard_by_is
]
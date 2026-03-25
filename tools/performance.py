from typing import Dict, List, Optional, Any
from analitycs.performance import analyze_inside_sales
from connections.netsuite_querys import get_op_so_data
from connections.postgresql_querys import get_scorecard_by_is_daily, get_scorecard_by_is_month, get_scorecard_by_is_year
from utils.date import get_month_start_and_today
from utils.json_df import save_result_to_json
from utils.envelope import build_tool_response
from connections.netsuite import NetSuiteConnection
from analitycs.data_transformations import tuple_to_dataframe
from connections.postgresql import execute_pg_query_dev

def get_inside_sales_performance_report(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> Dict[str, Any]:
    """Analyze Inside Sales performance for the selected period (Response time, hitrate).
    If no dates are provided, defaults to today's date.

    Args:
        initial_date: ISO-8601 string indicating the start date; defaults to today's date.
        final_date: ISO-8601 string indicating the end date; defaults to today's date.

    Returns:
        Dict[str, Any]: Inside Sales performance (Response time, hitrate, scorecard).
    """
    
    # Resolve default dates
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or today_date
    final_q_date = final_date or today_date

    
    sql = get_op_so_data(start_q_date, final_q_date)
    print("sql", sql)
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql)
    
    dataset_reference = save_result_to_json(columns, rows, f"Inside Sales Performance dataset between {initial_date} and {final_date}", name="op_to_so")

    df = tuple_to_dataframe(columns, rows)
    results = analyze_inside_sales(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_inside_sales_performance_report",
        summary=results,
        filters={
            "initial_date": start_q_date,
            "final_date": final_q_date,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

def get_scorecard_by_is(inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve the scorecard metrics by Inside Sales Daily, Monthly and Yearly.
    
    Use this tool when user asks for scorecard metrics by Inside Sales.

    Args:
        inside_sales: Optional[str] - Inside Sales name to filter by.
    Returns:
        Dict[str, Any]: Scorecard by IS.
    """
    sql_monthly = get_scorecard_by_is_month(inside_sales=inside_sales)
    columns_monthly, rows_monthly = execute_pg_query_dev(sql_monthly)
    df_monthly = tuple_to_dataframe(columns_monthly, rows_monthly)
    monthly_data = df_monthly.to_dict(orient="records")
    
    sql_daily = get_scorecard_by_is_daily(inside_sales=inside_sales)
    columns_daily, rows_daily = execute_pg_query_dev(sql_daily)
    df_daily = tuple_to_dataframe(columns_daily, rows_daily)
    daily_data = df_daily.to_dict(orient="records")
    
    sql_yearly = get_scorecard_by_is_year(inside_sales=inside_sales)
    columns_yearly, rows_yearly = execute_pg_query_dev(sql_yearly)
    df_yearly = tuple_to_dataframe(columns_yearly, rows_yearly)
    yearly_data = df_yearly.to_dict(orient="records")

    return build_tool_response(
        tool_name="get_scorecard_by_is",
        summary={
            "monthly_scorecard": monthly_data,
            "daily_scorecard": daily_data,
            "yearly_scorecard": yearly_data,
        },
        filters={
            "inside_sales": inside_sales,
        },
        source_systems=["postgresql"],
        details={
            "row_counts": {
                "monthly": len(rows_monthly),
                "daily": len(rows_daily),
                "yearly": len(rows_yearly),
            },
        },
    )
    
PERFORMANCE_TOOLS: List = [
    get_inside_sales_performance_report,
    get_scorecard_by_is
]

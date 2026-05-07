from typing import Any, Dict, Optional

from connections.postgresql.client import execute_pg_query_dev
from connections.postgresql.queries import get_scorecard_by_is_daily, get_scorecard_by_is_month, get_scorecard_by_is_year
from utils.envelope import build_tool_response
from utils.transformations import tuple_to_dataframe


def execute(inside_sales: Optional[str]) -> Dict[str, Any]:
    sql_monthly = get_scorecard_by_is_month(inside_sales=inside_sales)
    columns_monthly, rows_monthly = execute_pg_query_dev(sql_monthly)
    monthly_data = tuple_to_dataframe(columns_monthly, rows_monthly).to_dict(orient="records")

    sql_daily = get_scorecard_by_is_daily(inside_sales=inside_sales)
    columns_daily, rows_daily = execute_pg_query_dev(sql_daily)
    daily_data = tuple_to_dataframe(columns_daily, rows_daily).to_dict(orient="records")

    sql_yearly = get_scorecard_by_is_year(inside_sales=inside_sales)
    columns_yearly, rows_yearly = execute_pg_query_dev(sql_yearly)
    yearly_data = tuple_to_dataframe(columns_yearly, rows_yearly).to_dict(orient="records")

    return build_tool_response(
        tool_name="get_scorecard_by_is",
        summary={
            "monthly_scorecard": monthly_data,
            "daily_scorecard": daily_data,
            "yearly_scorecard": yearly_data,
        },
        filters={"inside_sales": inside_sales},
        source_systems=["postgresql"],
        details={"row_counts": {"monthly": len(rows_monthly), "daily": len(rows_daily), "yearly": len(rows_yearly)}},
    )

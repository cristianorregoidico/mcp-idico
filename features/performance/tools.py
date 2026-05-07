from typing import Any, Dict, List, Optional

from features.performance.use_cases import inside_sales_report, scorecard
from utils.date import get_month_start_and_today


def get_inside_sales_performance_report(initial_date: Optional[str] = None, final_date: Optional[str] = None) -> Dict[str, Any]:
    """Analyze Inside Sales performance for the selected period (Response time, hitrate)."""
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    return inside_sales_report.execute(start_q_date, final_q_date)


def get_scorecard_by_is(inside_sales: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve scorecard metrics by Inside Sales Daily/Monthly/Yearly."""
    return scorecard.execute(inside_sales)


PERFORMANCE_TOOLS: List = [
    get_inside_sales_performance_report,
    get_scorecard_by_is,
]

from typing import Any, Dict, List, Optional

from features.operations.use_cases import guides, imports, otd
from utils.date import get_month_start_and_today


def get_helga_guides(po: Optional[str] = None, status: Optional[str] = None, service: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve helga guides based on po, status and service filters."""
    return guides.execute(po, status, service)


def get_otd_indicators(initial_date: Optional[str] = None, final_date: Optional[str] = None, so_number: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve the on time delivery indicators by period."""
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    return otd.execute(start_q_date, final_q_date, so_number)


def get_customer_imports(customer_name: str) -> Dict[str, Any]:
    """Retrieve the imports summary for a given customer."""
    return imports.execute(customer_name)


OPS_TOOLS: List = [
    get_helga_guides,
    get_otd_indicators,
    get_customer_imports,
]

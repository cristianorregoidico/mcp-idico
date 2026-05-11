from typing import Any, Dict, List, Optional

from features.operations.use_cases import guides, imports, otd, purchase_orders
from utils.date import get_month_start_and_today


def get_helga_guides(po: Optional[str] = None, status: Optional[str] = None, service: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve Helga guides using PO, status and service filters.

    Use this tool when the user asks for shipment guide tracking,
    pending guides, or operational status by PO or service.

    Args:
        po: Purchase order filter; optional.
        status: Shipment status filter; optional.
        service: Service filter; optional.

    Returns:
        Dict[str, Any]: Result count, guide details and dataset reference.
    """
    return guides.execute(po, status, service)


def get_otd_indicators(initial_date: Optional[str] = None, final_date: Optional[str] = None, so_number: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve on-time-delivery indicators for a period or sales order.

    Use this tool when the user asks for OTD metrics, delivery compliance,
    or details for a specific sales order.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        so_number: Sales order filter; optional.

    Returns:
        Dict[str, Any]: OTD KPIs, optional sales-order detail and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    return otd.execute(start_q_date, final_q_date, so_number)


def get_customer_imports(customer_name: str) -> Dict[str, Any]:
    """Retrieve the imports summary for a given customer.

    Use this tool when the user asks for customer import amounts,
    trends, brands or vendor-related import summaries.

    Args:
        customer_name: Customer name; required.

    Returns:
        Dict[str, Any]: Import summary metrics and grouped business insights.
    """
    return imports.execute(customer_name)


def get_purchase_orders(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
    topic: str = "vendors",
) -> Dict[str, Any]:
    """Retrieve purchase-order analytics from NetSuite.

    Use this tool when the user asks for purchase-order status, vendor activity,
    overdue deliveries, rescheduling behavior, or item-level receipt and pending
    analysis for a given period.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start
            when either date is missing.
        final_date: End date in YYYY-MM-DD format; defaults to today when either
            date is missing.
        vendor: Partial vendor-name filter; optional.
        status: Purchase-order status filter; optional. Must be one of the
            allowed NetSuite PO statuses handled by the use case ("Pending Supervisor Approval","Pending Receipt","Rejected by Supervisor","Partially Received","Pending Billing/Partially Received","Pending Bill","Fully Billed",).
        brand: Partial item-brand filter; optional.
        topic: Analysis perspective; `vendors` for PO-level operational and
            financial metrics, or `items` for line-level receipt and pending
            metrics. Defaults to `vendors`.

    Returns:
        Dict[str, Any]: MCP response envelope with summary KPIs, applied filters,
        source metadata, and dataset reference for the queried purchase orders.
    """
    return purchase_orders.execute(initial_date, final_date, vendor, status, brand, topic)


OPS_TOOLS: List = [
    get_helga_guides,
    get_otd_indicators,
    get_customer_imports,
    get_purchase_orders,
]

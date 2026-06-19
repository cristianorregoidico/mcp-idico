from typing import Any, Dict, List, Optional

from features.financial.use_cases import aging, intercompany, prepayment
from utils.date import get_month_start_and_today


def get_aging(
    topic: str,
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    entity_name: Optional[str] = "",
    subsidiary: Optional[str] = "",
) -> Dict[str, Any]:
    """Retrieve accounts receivable or accounts payable aging KPIs for the selected period.

    Use this tool when the user asks for accounts receivable aging,
    accounts payable aging, cartera por cobrar, cuentas por pagar,
    customer or vendor overdue documents, collections priority,
    payment priorities, open AR or AP balances, or unapplied payments
    and credits.

    Args:
        topic: "receivable" for AR (customers) or "payable" for AP (vendors).
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        entity_name: Customer or vendor name substring to filter; optional.
        subsidiary: NetSuite subsidiary id ("3" Colombia, "4" Peru, "5" USA); optional.

    Returns:
        Dict[str, Any]: Aging buckets, totals by currency and subsidiary,
        top entities by open balance, concentration, housekeeping block
        with unapplied payments and credits, and dataset reference.
    """
    normalized_topic = (topic or "").lower().strip()
    if normalized_topic not in ("receivable", "payable"):
        raise ValueError(
            f"Invalid topic '{topic}'. Use 'receivable' (AR) or 'payable' (AP)."
        )

    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_entity_name = entity_name.upper() if entity_name else ""
    normalized_subsidiary = subsidiary.strip() if subsidiary else ""

    if normalized_topic == "receivable":
        return aging.execute_receivable_aging(
            start_q_date, final_q_date, normalized_entity_name, normalized_subsidiary
        )
    return aging.execute_payable_aging(
        start_q_date, final_q_date, normalized_entity_name, normalized_subsidiary
    )


def get_prepayments(
    topic: str,
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    entity_name: Optional[str] = "",
    subsidiary: Optional[str] = "",
) -> Dict[str, Any]:
    """Retrieve customer deposits or vendor prepayments and their applications.

    Use this tool when the user asks about customer deposits, customer advances,
    vendor prepayments, supplier advances, anticipos de clientes, anticipos de
    proveedores, deposits, deposit applications, prepayments, or prepayment
    applications.

    The tool provides financial visibility over money received or paid before
    the final document is fully consumed or applied. It focuses on entity,
    amount, currency, subsidiary, status, movement type, and concentration.

    This tool does not resolve full document-level traceability from each
    prepayment to the exact invoice, bill, sales order, or purchase order.
    For document-level drill-down, the user should review the transaction
    directly in NetSuite.

    Args:
        topic: "customer" for customer deposits or "vendor" for vendor prepayments.
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        entity_name: Customer or vendor name substring to filter; optional.
        subsidiary: NetSuite subsidiary id ("3" Colombia, "4" Peru, "5" USA); optional.

    Returns:
        Dict[str, Any]: Prepayment or deposit summary by movement type, currency,
        subsidiary, status, transaction type, top entities, concentration,
        data quality notes, and dataset reference.
    """
    normalized_topic = (topic or "").lower().strip()
    if normalized_topic not in ("customer", "vendor"):
        raise ValueError(f"Invalid topic '{topic}'. Use 'customer' or 'vendor'.")

    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_entity_name = entity_name.upper() if entity_name else ""
    normalized_subsidiary = subsidiary.strip() if subsidiary else ""

    return prepayment.execute_prepayment_analysis(
        topic=normalized_topic,
        initial_date=start_q_date,
        final_date=final_q_date,
        entity_name=normalized_entity_name,
        subsidiary=normalized_subsidiary,
    )


def get_intercompany_balances(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    entity_name: Optional[str] = "",
    subsidiary: Optional[str] = "",
) -> Dict[str, Any]:
    """Retrieve intercompany balances and transaction movements between subsidiaries.

    Use this tool when the user asks about intercompany balances, triangulation,
    subsidiary-to-subsidiary transactions, related company balances, saldos entre
    subsidiarias, transacciones intercompany, triangulacion, or who owes whom
    between IDICO companies.

    The tool provides visibility over two separate views:
    open balances and transaction movement. Open balances are based on unpaid
    or unused amounts. Transaction movement is based on accounting and foreign
    amount activity between the accounting subsidiary and the intercompany entity.

    This tool does not replace a formal intercompany reconciliation. It provides
    analytical visibility into relationships, movements, open balances, transaction
    types, accounts, and top open documents.

    Args:
        initial_date: Start date in YYYY-MM-DD format; defaults to month start.
        final_date: End date in YYYY-MM-DD format; defaults to today.
        entity_name: Intercompany entity name substring to filter; optional.
        subsidiary: NetSuite subsidiary id ("3" Colombia, "4" Peru, "5" USA); optional.

    Returns:
        Dict[str, Any]: Intercompany open balance summary, movement summary,
        transaction scope distribution, transaction type distribution, account
        type distribution, compensation summary, top open balance documents,
        data quality notes, and dataset reference.
    """
    start_of_month, today_date = get_month_start_and_today()
    start_q_date = initial_date or start_of_month
    final_q_date = final_date or today_date
    normalized_entity_name = entity_name.upper() if entity_name else ""
    normalized_subsidiary = subsidiary.strip() if subsidiary else ""

    return intercompany.execute_intercompany_analysis(
        initial_date=start_q_date,
        final_date=final_q_date,
        entity_name=normalized_entity_name,
        subsidiary=normalized_subsidiary,
    )


FINANCIAL_TOOLS: List = [
    get_aging,
    get_prepayments,
    get_intercompany_balances,
]

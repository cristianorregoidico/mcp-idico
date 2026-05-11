from typing import Any, Dict, Optional

from connections.netsuite.client import NetSuiteConnection
from features.operations.domain.purchase_orders import build_items_analysis, build_vendors_analysis, normalize_topic
from features.operations.queries.purchase_orders import get_purchase_orders_query
from utils.date import get_month_start_and_today
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe

ALLOWED_STATUS = {
    "Pending Supervisor Approval",
    "Pending Receipt",
    "Rejected by Supervisor",
    "Partially Received",
    "Pending Billing/Partially Received",
    "Pending Bill",
    "Fully Billed",
}


def execute(
    initial_date: Optional[str] = None,
    final_date: Optional[str] = None,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
    topic: Optional[str] = "vendors",
) -> Dict[str, Any]:
    if not initial_date or not final_date:
        initial_date, final_date = get_month_start_and_today()

    normalized_topic = normalize_topic(topic)

    if status and status not in ALLOWED_STATUS:
        raise ValueError(f"Invalid status '{status}'. Allowed values: {sorted(ALLOWED_STATUS)}")

    sql, params = get_purchase_orders_query(
        initial_date=initial_date,
        final_date=final_date,
        vendor=vendor,
        status=status,
        brand=brand,
    )

    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(
        columns,
        rows,
        f"Purchase orders dataset between {initial_date} and {final_date}",
        name="purchase_orders_data",
    )
    df = tuple_to_dataframe(columns, rows)

    if normalized_topic == "items":
        summary = build_items_analysis(df)
    else:
        summary = build_vendors_analysis(df)

    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_purchase_orders_analysis",
        summary=summary,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            "vendor": vendor,
            "status": status,
            "brand": brand,
            "topic": normalized_topic,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

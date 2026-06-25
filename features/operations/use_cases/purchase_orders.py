from typing import Any, Dict, Optional

from connections.postgresql.client import execute_pg_query
from features.operations.domain.purchase_orders import build_items_analysis, build_vendors_analysis, build_walle_analysis, normalize_topic
from features.operations.queries.purchase_orders import get_purchase_orders_query_pg, get_purchase_orders_walle_query_pg
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
    normalized_vendor = (vendor or "").strip() or None
    normalized_status = (status or "").strip() or None
    normalized_brand = (brand or "").strip() or None

    if normalized_status and normalized_status not in ALLOWED_STATUS:
        raise ValueError(f"Invalid status '{normalized_status}'. Allowed values: {sorted(ALLOWED_STATUS)}")

    if normalized_topic == "walle":
        sql, params = get_purchase_orders_walle_query_pg(
            initial_date=initial_date,
            final_date=final_date,
            vendor=normalized_vendor,
            status=normalized_status,
            brand=normalized_brand,
        )
    else:
        sql, params = get_purchase_orders_query_pg(
            initial_date=initial_date,
            final_date=final_date,
            vendor=normalized_vendor,
            status=normalized_status,
            brand=normalized_brand,
        )

    columns, rows = execute_pg_query(sql, params)

    dataset_reference = save_result_to_json(
        columns,
        rows,
        f"Purchase orders dataset between {initial_date} and {final_date}",
        name="purchase_orders_data",
    )
    df = tuple_to_dataframe(columns, rows)

    if normalized_topic == "items":
        summary = build_items_analysis(df)
    elif normalized_topic == "walle":
        summary = build_walle_analysis(df)
    else:
        summary = build_vendors_analysis(df)

    summary.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_purchase_orders",
        summary=summary,
        filters={
            "initial_date": initial_date,
            "final_date": final_date,
            "vendor": normalized_vendor,
            "status": normalized_status,
            "brand": normalized_brand,
            "topic": normalized_topic,
        },
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

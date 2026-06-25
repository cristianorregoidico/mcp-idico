from typing import Any, List, Optional, Tuple


def _contains_param(value: str) -> str:
    return f"%{value}%"


def get_purchase_orders_query(
    initial_date: str,
    final_date: str,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    where_clauses = [
        "t.type = 'PurchOrd'",
        "tl.mainline = 'F'",
        "tl.itemtype IN ('InvtPart', 'Service')",
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "ts.id NOT IN ('P', 'H', 'Y')"
    ]
    params: List[Any] = [initial_date, final_date]

    if vendor:
        where_clauses.append("BUILTIN.DF(t.entity) LIKE '%' || UPPER(?) || '%'")
        params.append(vendor)

    if status:
        where_clauses.append("ts.name LIKE '%' || ? || '%'")
        params.append(status)

    if brand:
        where_clauses.append("BUILTIN.DF(i.custitem13) LIKE '%' || UPPER(?) || '%'")
        params.append(brand)

    sql = f"""
    SELECT
        t.id AS po_id,
        t.tranid AS po_number,
        TO_CHAR(t.createddate, 'YYYY-MM-DD') AS created_date,
        TO_CHAR(t.trandate, 'YYYY-MM-DD') AS po_date,
        TO_CHAR(t.trandate, 'YYYY-MM') AS po_period,
        TO_CHAR(t.duedate, 'YYYY-MM-DD') AS receive_by,
        TO_CHAR(t.custbody_evol_new_receive_by, 'YYYY-MM-DD') AS new_receive_by,
        TO_CHAR(t.custbody_evol_last_delivery_date, 'YYYY-MM-DD') AS last_est_delivery_date_informed,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS today,
        ts.name AS status,
        BUILTIN.DF(t.approvalstatus) AS approval_status,
        BUILTIN.DF(t.custbody_evol_expediting_status) AS expediting_status,
        BUILTIN.DF(t.custbody13) AS payment_status,
        BUILTIN.DF(t.entity) AS vendor,
        BUILTIN.DF(t.terms) AS terms,
        BUILTIN.DF(t.custbody41) AS incoterms,
        BUILTIN.DF(ea.country) AS vendor_country,
        BUILTIN.DF(tl.subsidiary) AS subsidiary,
        BUILTIN.DF(t.currency) AS currency,
        -t.foreigntotal * t.exchangerate AS amount_usd,
        so.tranid AS so_number,
        BUILTIN.DF(so.entity) AS customer,
        BUILTIN.DF(tl.item) AS item,
        SUBSTR(i.purchasedescription, 1, 100) AS item_description,
        tl.itemtype AS item_type,
        tl.quantity AS quantity,
        tl.quantityshiprecv AS quantity_received,
        tl.rate AS unit_rate,
        tl.foreignamount * t.exchangerate AS line_amount,
        BUILTIN.DF(i.custitem13) AS brand,
        BUILTIN.DF(i.class) AS product_group
    FROM transaction t
    INNER JOIN transactionline tl
        ON tl.transaction = t.id
    LEFT JOIN transactionStatus ts
        ON ts.id = t.status
        AND ts.trantype = 'PurchOrd'
    LEFT JOIN item i
        ON i.id = tl.item
    LEFT JOIN entityAddressBook eab
        ON eab.entity = t.entity
        AND eab.defaultbilling = 'T'
    LEFT JOIN EntityAddress ea
        ON ea.nkey = eab.addressbookaddress
    LEFT JOIN PreviousTransactionLink po_to_so
        ON po_to_so.nextdoc = t.id
        AND po_to_so.linktype = 'SpecOrd'
    LEFT JOIN transaction so
        ON so.id = po_to_so.previousdoc
        AND so.type = 'SalesOrd'
    WHERE {' AND '.join(where_clauses)}
    ORDER BY
        t.trandate DESC,
        t.tranid,
        tl.id
    """

    return sql, params


def get_purchase_orders_query_pg(
    initial_date: str,
    final_date: str,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    where_clauses = [
        "po.po_date BETWEEN %s::date AND %s::date",
        "po.po_status NOT IN ('Undefined', 'Closed', 'Planned')",
    ]
    params: List[Any] = [initial_date, final_date]

    if vendor:
        where_clauses.append("po.vendor_name ILIKE %s")
        params.append(_contains_param(vendor))

    if status:
        where_clauses.append("po.po_status = %s")
        params.append(status)

    if brand:
        where_clauses.append("i.brand ILIKE %s")
        params.append(_contains_param(brand))

    sql = f"""
    SELECT
        po.po_id,
        po.po_name AS po_number,
        po.created_date::date,
        po.po_date,
        TO_CHAR(po.po_date, 'YYYY-MM') AS po_period,
        po.receive_by,
        po.new_receive_by,
        po.last_est_delivery_date_informed,
        CURRENT_DATE AS today,
        po.po_status AS status,
        po.approval_status,
        po.expediting_status,
        po.payment_status,
        po.vendor_name AS vendor,
        po.terms,
        po.incoterms_2020 AS incoterms,
        po.vendor_country,
        po.subsidiary,
        (SELECT poi.transaction_currency FROM ods.walle.purchaseorder_items poi
         WHERE poi.po_id = po.po_id
         LIMIT 1) AS currency,
        po.amount_usd,
        so.so_name AS so_number,
        so.customer_name AS customer,
        i.part_number AS item,
        i.description AS item_description,
        i.item_type,
        i.qty AS quantity,
        i.received AS quantity_received,
        i.unit_price AS unit_rate,
        i.line_amount,
        i.brand,
        i.product_group
    FROM ods.walle.purchaseorder po
    LEFT JOIN ods.walle.salesorder so ON so.so_id = po.created_from
    JOIN ods.walle.purchaseorder_items i ON i.po_id = po.po_id
    WHERE {' AND '.join(where_clauses)}
    ORDER BY
        po.po_date DESC,
        po.po_name,
        i.po_id
    """

    return sql, params


def get_purchase_orders_walle_query_pg(
    initial_date: str,
    final_date: str,
    vendor: Optional[str] = None,
    status: Optional[str] = None,
    brand: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    where_clauses = [
        "po.po_date BETWEEN %s::date AND %s::date",
        "po.po_status NOT IN ('Undefined', 'Closed', 'Planned')",
    ]
    params: List[Any] = [initial_date, final_date]

    if vendor:
        where_clauses.append("po.vendor_name ILIKE %s")
        params.append(_contains_param(vendor))

    if status:
        where_clauses.append("po.po_status = %s")
        params.append(status)

    if brand:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM ods.walle.purchaseorder_items poi WHERE poi.po_id = po.po_id AND poi.brand ILIKE %s)"
        )
        params.append(_contains_param(brand))

    sql = f"""
    WITH po_base AS (
        SELECT
            po.po_id,
            po.po_name AS po_number,
            po.created_date::date,
            po.po_date,
            TO_CHAR(po.po_date, 'YYYY-MM') AS po_period,
            po.receive_by,
            po.new_receive_by,
            po.last_est_delivery_date_informed,
            CURRENT_DATE AS today,
            po.po_status AS status,
            po.approval_status,
            po.expediting_status,
            po.payment_status,
            po.vendor_name AS vendor,
            po.vendor_country,
            po.subsidiary,
            po.amount_usd
        FROM ods.walle.purchaseorder po
        WHERE {' AND '.join(where_clauses)}
    ),
    email_agg AS (
        SELECT
            poe.po_id,
            COUNT(*) AS email_count,
            COUNT(*) FILTER (WHERE poe.email_type = 'INBOUND') AS inbound_email_count,
            COUNT(*) FILTER (WHERE poe.email_type = 'OUTBOUND') AS outbound_email_count,
            COUNT(*) FILTER (WHERE poe.summarized IS TRUE) AS ai_processed_email_count,
            MIN(poe.email_date) AS first_email_date,
            MAX(poe.email_date) AS last_email_date
        FROM ods.walle.purchaseorder_email poe
        GROUP BY poe.po_id
    ),
    suggestion_agg AS (
        SELECT
            eas.po_id,
            COUNT(*) AS suggestion_row_count,
            COUNT(*) FILTER (WHERE eas.manual IS NOT NULL) AS processed_suggestion_count,
            COUNT(*) FILTER (WHERE eas.manual IS NULL) AS unprocessed_suggestion_count,
            COUNT(*) FILTER (WHERE eas.manual IS TRUE) AS suggestion_true_count,
            COUNT(*) FILTER (WHERE eas.manual IS FALSE) AS suggestion_false_count,
            COALESCE(SUM(jsonb_array_length(eas.actions)), 0) AS total_actions_suggested,
            AVG(jsonb_array_length(eas.actions)::numeric) AS avg_actions_per_suggestion,
            MIN(eas."createdAt") AS first_suggestion_at,
            MAX(eas."createdAt") AS last_suggestion_at
        FROM ods.walle.expediting_action_suggestions eas
        GROUP BY eas.po_id
    ),
    finance_agg AS (
        SELECT
            fi.po_id,
            COUNT(*) AS finance_request_count,
            COUNT(*) FILTER (WHERE fi.status = 'PENDING') AS finance_pending_count,
            COUNT(*) FILTER (WHERE fi.status = 'COMPLETED') AS finance_completed_count,
            COUNT(*) FILTER (WHERE fi.status = 'CANCELLED') AS finance_cancelled_count,
            COUNT(*) FILTER (WHERE jsonb_array_length(fi.payment_files) > 0) AS finance_executed_count,
            COUNT(*) FILTER (WHERE jsonb_array_length(fi.files) > 0) AS finance_with_support_files_count,
            COUNT(*) FILTER (WHERE fi.razon_rejection IS NOT NULL) AS finance_rejection_event_count,
            COUNT(*) FILTER (WHERE fi.razon_anulation IS NOT NULL) AS finance_anulation_event_count,
            COUNT(*) FILTER (WHERE fi.type = 'ANTICIPO') AS anticipo_count,
            COUNT(*) FILTER (WHERE fi.type = 'GASTO_IMPORTACION') AS gasto_importacion_count,
            MIN(fi."createdAt") AS first_finance_created_at,
            MAX(fi."updatedAt") AS last_finance_updated_at
        FROM ods.walle.finanzas_items fi
        GROUP BY fi.po_id
    )
    SELECT
        pb.po_id,
        pb.po_number,
        pb.created_date,
        pb.po_date,
        pb.po_period,
        pb.receive_by,
        pb.new_receive_by,
        pb.last_est_delivery_date_informed,
        pb.today,
        pb.status,
        pb.approval_status,
        pb.expediting_status,
        pb.payment_status,
        pb.vendor,
        pb.vendor_country,
        pb.subsidiary,
        pb.amount_usd,
        COALESCE(ea.email_count, 0) AS email_count,
        COALESCE(ea.inbound_email_count, 0) AS inbound_email_count,
        COALESCE(ea.outbound_email_count, 0) AS outbound_email_count,
        COALESCE(ea.ai_processed_email_count, 0) AS ai_processed_email_count,
        ea.first_email_date,
        ea.last_email_date,
        COALESCE(sa.suggestion_row_count, 0) AS suggestion_row_count,
        COALESCE(sa.processed_suggestion_count, 0) AS processed_suggestion_count,
        COALESCE(sa.unprocessed_suggestion_count, 0) AS unprocessed_suggestion_count,
        COALESCE(sa.suggestion_true_count, 0) AS suggestion_true_count,
        COALESCE(sa.suggestion_false_count, 0) AS suggestion_false_count,
        COALESCE(sa.total_actions_suggested, 0) AS total_actions_suggested,
        COALESCE(sa.avg_actions_per_suggestion, 0) AS avg_actions_per_suggestion,
        sa.first_suggestion_at,
        sa.last_suggestion_at,
        COALESCE(fa.finance_request_count, 0) AS finance_request_count,
        COALESCE(fa.finance_pending_count, 0) AS finance_pending_count,
        COALESCE(fa.finance_completed_count, 0) AS finance_completed_count,
        COALESCE(fa.finance_cancelled_count, 0) AS finance_cancelled_count,
        COALESCE(fa.finance_executed_count, 0) AS finance_executed_count,
        COALESCE(fa.finance_with_support_files_count, 0) AS finance_with_support_files_count,
        COALESCE(fa.finance_rejection_event_count, 0) AS finance_rejection_event_count,
        COALESCE(fa.finance_anulation_event_count, 0) AS finance_anulation_event_count,
        COALESCE(fa.anticipo_count, 0) AS anticipo_count,
        COALESCE(fa.gasto_importacion_count, 0) AS gasto_importacion_count,
        fa.first_finance_created_at,
        fa.last_finance_updated_at
    FROM po_base pb
    LEFT JOIN email_agg ea ON ea.po_id = pb.po_id
    LEFT JOIN suggestion_agg sa ON sa.po_id = pb.po_id
    LEFT JOIN finance_agg fa ON fa.po_id = pb.po_id
    ORDER BY pb.po_date DESC, pb.po_number
    """

    return sql, params

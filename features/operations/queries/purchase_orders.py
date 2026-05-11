from typing import Any, List, Optional, Tuple


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

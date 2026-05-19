def _build_po_or_date_filter(
    *,
    date_column: str,
    initial_date: str | None,
    final_date: str | None,
    po_name: str | None,
) -> tuple[str, list[str]]:
    if po_name:
        return "po.po_name = %s", [po_name]

    return f"{date_column} BETWEEN %s AND %s", [initial_date, final_date]


def get_summarized_emails_query(
    initial_date: str | None = None,
    final_date: str | None = None,
    po_name: str | None = None,
) -> tuple[str, list[str | None]]:
    dynamic_where, params = _build_po_or_date_filter(
        date_column="poe.email_date::date",
        initial_date=initial_date,
        final_date=final_date,
        po_name=po_name,
    )

    sql = f"""
    SELECT
        po.po_name,
        po.po_status,
        po.vendor_name,
        po.last_expediting_date,
        po.receive_by,
        po.new_receive_by,
        poe.email_id,
        poe.email_date,
        poe.subject,
        poe.email_type
    FROM ods.walle.purchaseorder po
    INNER JOIN ods.walle.purchaseorder_email poe
        ON po.po_id = poe.po_id
    INNER JOIN ods.walle.model_email_logistics_summary emls
        ON poe.email_id = emls.email_id
    WHERE {dynamic_where}
    ORDER BY poe.email_date NULLS LAST;
    """
    return sql, params


def get_analyzed_emails_query(
    initial_date: str | None = None,
    final_date: str | None = None,
    po_name: str | None = None,
) -> tuple[str, list[str | None]]:
    dynamic_where, params = _build_po_or_date_filter(
        date_column='mea."createdAt"::date',
        initial_date=initial_date,
        final_date=final_date,
        po_name=po_name,
    )

    sql = f"""
    SELECT
        po.po_name,
        mea.id,
        mea.email_id,
        mea.version,
        mea.scenario,
        mea.confidence,
        mea.initial_info,
        mea.db_updates,
        mea.extracted,
        mea.notes,
        mea."createdAt",
        mea.llm_meta,
        mea.actions,
        mea.flags,
        mea.evidence
    FROM ods.walle.purchaseorder po
    INNER JOIN ods.walle.purchaseorder_email poe
        ON po.po_id = poe.po_id
    INNER JOIN ods.walle.model_expediting_analysis mea
        ON poe.email_id = mea.email_id
    WHERE {dynamic_where}
    ORDER BY mea."createdAt" NULLS LAST;
    """
    return sql, params


def get_action_suggestions_query(
    initial_date: str | None = None,
    final_date: str | None = None,
    po_name: str | None = None,
) -> tuple[str, list[str | None]]:
    dynamic_where, params = _build_po_or_date_filter(
        date_column='eas."createdAt"::date',
        initial_date=initial_date,
        final_date=final_date,
        po_name=po_name,
    )

    sql = f"""
    SELECT
        eas.po_id,
        po.po_name,
        eas.actions,
        eas."createdAt",
        eas."updatedAt"
    FROM ods.walle.expediting_action_suggestions eas
    LEFT JOIN ods.walle.purchaseorder po
        ON po.po_id = eas.po_id
    WHERE eas.manual IS NOT NULL
      AND {dynamic_where}
    ORDER BY eas."createdAt" NULLS LAST;
    """
    return sql, params


def get_walle_event_log_query(initial_date: str, final_date: str) -> tuple[str, list[str]]:
    sql = """
    SELECT
        el.method,
        el.endpoint,
        el.status_code,
        el.duration_ms,
        el."createdAt"
    FROM ods.walle.event_log el
    WHERE el.method = 'POST'
      AND el."createdAt"::date BETWEEN %s AND %s
    ORDER BY el."createdAt" DESC;
    """
    return sql, [initial_date, final_date]

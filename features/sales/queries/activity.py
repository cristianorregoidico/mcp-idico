def get_calls_summary(start_date: str, final_date: str, customer_name: str = '', organizer: str = '', subject: str = '') -> tuple[str, list[str]]:
    where_clauses = [
        "activity_date >= %s",
        "activity_date < %s",
        "description IS NOT NULL",
        "LENGTH(TRIM(description)) >= 500",
    ]
    params: list[str] = [start_date, final_date]

    if customer_name:
        where_clauses.append("account ILIKE %s")
        params.append(f"%{customer_name}%")
    if organizer:
        where_clauses.append("organizer ILIKE %s")
        params.append(f"%{organizer}%")
    if subject:
        where_clauses.append("subject ILIKE %s")
        params.append(f"%{subject}%")

    sql = f"""
    SELECT
    activity_date,
    subject,
    account,
    organizer,
    attendees,
    contact,
    description
FROM ods.analytics.dataset_modjo_idra
WHERE {' AND '.join(where_clauses)}
ORDER BY activity_date DESC;
    """
    return sql, params

def get_idra_usage_query(initial_date: str, final_date_exclusive: str) -> tuple[str, list[str]]:
    sql = """
    SELECT
        tool_name,
        username,
        response,
        duration_ms,
        created_at
    FROM ods.analytics.idra_tool_calls
    WHERE username <> 'Not Identified'
      AND created_at >= %s
      AND created_at < %s
    ORDER BY created_at DESC;
    """
    return sql, [initial_date, final_date_exclusive]

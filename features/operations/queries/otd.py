def get_on_time_delivery(initial_date: str, final_date: str, so_number: str = None) -> tuple[str, list[str]]:
    if so_number:
        return """
        SELECT *
        FROM ods.analytics.tableau_otd otd
        WHERE otd.so_doc_number = %s;
        """, [so_number]

    return """
    SELECT
    *
    FROM ods.analytics.tableau_otd otd
    WHERE to_date(otd.if_create_date, 'YYYY/MM/DD')
        BETWEEN %s::date AND %s::date;
    """, [initial_date, final_date]

def get_scorecard_by_is_month(inside_sales: str = None) -> tuple[str, list[str]]:
    if inside_sales:
        return """
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_mensual WHERE sales_rep = %s;
        """, [inside_sales]
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_mensual;
    """, []


def get_scorecard_by_is_daily(inside_sales: str = None) -> tuple[str, list[str]]:
    if inside_sales:
        return """
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_diario WHERE sales_rep = %s;
        """, [inside_sales]
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_diario;
    """, []


def get_scorecard_by_is_year(inside_sales: str = None) -> tuple[str, list[str]]:
    if inside_sales:
        return """
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_anual WHERE sales_rep = %s;
        """, [inside_sales]
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_anual;
    """, []

from typing import Any, Dict

from connections.postgresql.client import execute_pg_query_dev
from features.sales.queries.activity import get_calls_summary
from utils.envelope import build_tool_response
from utils.transformations import tuple_to_dataframe


def execute(start_date: str, final_date: str, customer_name: str, organizer: str, subject: str) -> Dict[str, Any]:
    sql, params = get_calls_summary(start_date, final_date, customer_name, organizer, subject)
    columns, rows = execute_pg_query_dev(sql, params)
    df = tuple_to_dataframe(columns, rows)
    calls_summary = df.to_dict(orient="records")

    return build_tool_response(
        tool_name="get_relationships_insights",
        summary={"total_calls": len(rows)},
        filters={
            "start_date": start_date,
            "final_date": final_date,
            "customer_name": customer_name or "",
            "organizer": organizer or "",
            "subject": subject or "",
        },
        source_systems=["postgresql"],
        columns=columns,
        rows=rows,
        details={
            "suggested_prompt_for_insights": """
            A continuación se presentan los resúmenes de llamadas de ventas de IDICO. Tu objetivo es actuar como un Analista de Marketing Estratégico. Analiza los textos y responde exclusivamente basado en esta información:
            1. Identifica las 3 objeciones más recurrentes.
            2. Enumera qué características o valores mencionan que aprecian de IDICO.
            3. Describe quiénes suelen estar presentes (roles) según el campo attendees y la descripción.
            4. Clasifica las menciones en categorías (Precio, Servicio, Tecnología, etc.)""",
            "calls_summary": calls_summary,
        },
    )

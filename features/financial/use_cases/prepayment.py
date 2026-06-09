# Destino: mcp-idra/features/financial/use_cases/prepayment.py

from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.financial.domain.prepayment import summarize_prepayment_analysis
from features.financial.queries.prepayment import get_prepayment_data
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute_prepayment_analysis(
    topic: str,
    initial_date: str,
    final_date: str,
    entity_name: str = "",
    subsidiary: str = "",
) -> Dict[str, Any]:
    """
    Ejecuta el analisis de anticipos de clientes o proveedores.
    """
    topic = (topic or "").lower().strip()

    if topic not in ("vendor", "customer"):
        raise ValueError(f"Invalid topic '{topic}'. Expected 'vendor' or 'customer'.")

    sql, params = get_prepayment_data(
        topic=topic,
        initial_date=initial_date,
        final_date=final_date,
        entity_name=entity_name,
        subsidiary=subsidiary,
    )

    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_description = (
        f"{topic.capitalize()} prepayment dataset between "
        f"{initial_date} and {final_date}"
    )

    dataset_reference = save_result_to_json(
        columns,
        rows,
        dataset_description,
        name=f"{topic}_prepayment_data",
    )

    df = tuple_to_dataframe(columns, rows)

    results = summarize_prepayment_analysis(df, topic)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name=f"get_prepayments_{topic}",
        summary=results,
        filters={
            "topic": topic,
            "initial_date": initial_date,
            "final_date": final_date,
            "entity_name": entity_name or None,
            "subsidiary": subsidiary or None,
        },
        source_systems=["netsuite"],
        columns=columns,
        rows=rows,
        dataset_reference=dataset_reference,
    )

from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.financial.domain.intercompany import summarize_intercompany_analysis
from features.financial.queries.intercompany import get_intercompany_data
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute_intercompany_analysis(
    initial_date: str,
    final_date: str,
    entity_name: str = "",
    subsidiary: str = "",
) -> Dict[str, Any]:
    """
    Ejecuta el analisis de balances y movimientos intercompany.
    """
    sql, params = get_intercompany_data(
        initial_date=initial_date,
        final_date=final_date,
        entity_name=entity_name,
        subsidiary=subsidiary,
    )

    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_description = (
        "Intercompany balances and transactions dataset between "
        f"{initial_date} and {final_date}"
    )

    dataset_reference = save_result_to_json(
        columns,
        rows,
        dataset_description,
        name="intercompany_data",
    )

    df = tuple_to_dataframe(columns, rows)

    results = summarize_intercompany_analysis(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_intercompany_balances",
        summary=results,
        filters={
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

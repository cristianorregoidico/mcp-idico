# Destino: mcp-idra/features/financial/use_cases/aging.py

from typing import Any, Dict

from connections.netsuite.client import NetSuiteConnection
from features.financial.domain.aging import (
    summarize_payable_aging,
    summarize_receivable_aging,
)
from features.financial.queries.aging import get_aging_data
from utils.envelope import build_tool_response
from utils.json_df import save_result_to_json
from utils.transformations import tuple_to_dataframe


def execute_receivable_aging(
    initial_date: str, final_date: str, entity_name: str, subsidiary: str
) -> Dict[str, Any]:
    sql, params = get_aging_data(
        "receivable", initial_date, final_date, entity_name, subsidiary
    )
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(
        columns,
        rows,
        f"Receivable aging dataset between {initial_date} and {final_date}",
        name="receivable_aging_data",
    )

    df = tuple_to_dataframe(columns, rows)
    results = summarize_receivable_aging(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_aging_receivable",
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


def execute_payable_aging(
    initial_date: str, final_date: str, entity_name: str, subsidiary: str
) -> Dict[str, Any]:
    sql, params = get_aging_data(
        "payable", initial_date, final_date, entity_name, subsidiary
    )
    conn = NetSuiteConnection()
    with conn.managed() as ns:
        columns, rows = ns.execute_query(sql, params)

    dataset_reference = save_result_to_json(
        columns,
        rows,
        f"Payable aging dataset between {initial_date} and {final_date}",
        name="payable_aging_data",
    )

    df = tuple_to_dataframe(columns, rows)
    results = summarize_payable_aging(df)
    results.pop("full_data_reference", None)

    return build_tool_response(
        tool_name="get_aging_payable",
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

from typing import Any, Dict

from connections.postgresql.client import execute_pg_query_dev
from connections.postgresql.queries import (
    get_customer_country,
    get_vendors_country_brand,
    get_vendors_customer_brand,
)
from features.sales.domain.vendor_recommendation import analize_hr_desviado
from utils.envelope import build_tool_response
from utils.transformations import tuple_to_dataframe


def execute(customer_name: str, brand: str) -> Dict[str, Any]:
    sql_cus_brand = get_vendors_customer_brand(customer_name, brand)
    columns_cus_brand, rows_cus_brand = execute_pg_query_dev(sql_cus_brand)
    df_cus_brand = tuple_to_dataframe(columns_cus_brand, rows_cus_brand)

    sql_country = get_customer_country(customer_name)
    _, rows_country = execute_pg_query_dev(sql_country)
    country = rows_country[0][0] if rows_country else ""

    sql_country_brand = get_vendors_country_brand(country, brand)
    columns_country_brand, rows_country_brand = execute_pg_query_dev(sql_country_brand)
    df_country_brand = tuple_to_dataframe(columns_country_brand, rows_country_brand)

    summary = analize_hr_desviado(df_cus_brand, df_country_brand)

    return build_tool_response(
        tool_name="get_vendors_to_quote",
        summary=summary,
        filters={
            "customer_name": customer_name,
            "brand": brand,
            "customer_country": country or None,
        },
        source_systems=["postgresql"],
        columns=columns_cus_brand,
        rows=rows_cus_brand,
        details={
            "customer_brand_matches": {
                "row_count": len(rows_cus_brand),
                "columns": columns_cus_brand,
            },
            "country_brand_matches": {
                "row_count": len(rows_country_brand),
                "columns": columns_country_brand,
            },
        },
    )

def get_customer_imports_data(customer_name: str) -> tuple[str, list[str]]:
    return """
    SELECT * FROM ods.analytics.datasur WHERE importador LIKE %s;
    """, [f"%{customer_name}%"]

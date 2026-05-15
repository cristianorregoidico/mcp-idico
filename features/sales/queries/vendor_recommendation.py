def get_vendors_customer_brand(customer_name: str, brand: str) -> tuple[str, list[str]]:
    return """
    SELECT * FROM ods.analytics.hr_cus_brand_consolidado
    WHERE customer_name ILIKE %s
    AND brand ILIKE %s
    AND probabilidad > 0
    ORDER BY
    customer_name ASC,
    brand ASC,
    probabilidad DESC,
    count_so DESC;
    """, [f"%{customer_name}%", f"%{brand}%"]


def get_vendors_country_brand(country: str, brand: str) -> tuple[str, list[str]]:
    return """
    SELECT * FROM ods.analytics.hr_country_brand_consolidado
    WHERE country ILIKE %s
    AND brand ILIKE %s
    ORDER BY
        country ASC,
        brand ASC,
        probabilidad DESC,
        count_so DESC;
    """, [f"%{country}%", f"%{brand}%"]


def get_customer_country(customer_name: str) -> tuple[str, list[str]]:
    return """
    SELECT country FROM ods.analytics.hr_cus_brand_consolidado
    WHERE customer_name ILIKE %s
    LIMIT 1;
    """, [f"%{customer_name}%"]

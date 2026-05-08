def get_bookings_data(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> tuple[str, list[str]]:
    where_clauses = [
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "csr.subsidiary IN (5, 4, 3)",
        "t.type IN ('SalesOrd')",
        "ts.id NOT IN ('C', 'H', 'A', 'Y')",
        "t.entity NOT IN (37839, 3085, 213418,2414, 355535, 1066, 401658, 101528, 144291, 183866, 185705, 186223)",
        "t.custbody7 = 'F'",
    ]
    params = [initial_date, final_date]

    if customer_name:
        where_clauses.append("BUILTIN.DF(t.entity) LIKE ?")
        params.append(f"%{customer_name}%")

    if inside_sales:
        where_clauses.append("e.firstname || ' ' || e.lastname LIKE ?")
        params.append(f"%{inside_sales}%")

    sql = f"""
    SELECT
	t.tranid AS so_number,
	ts.name AS status,
	TO_CHAR(t.trandate, 'YYYY-MM-DD') AS date,
    TO_CHAR(t.trandate, 'YYYY-MM') AS period,
    CASE
    	WHEN csr.subsidiary = 5 AND t.custbody_transaccion_chile = 'T' THEN 'IDICO Chile'
    	ELSE BUILTIN.DF(csr.subsidiary)
    END AS subsidiary,
    BUILTIN.DF(t.currency) AS currency,
    BUILTIN.DF(t.entity) AS customer,
    BUILTIN.DF(ea.country) AS customer_country,
    CASE
        WHEN t.custbody_evol_incoterms IS NOT NULL THEN BUILTIN.DF(t.custbody_evol_incoterms)
        ELSE BUILTIN.DF(t.custbody_inc) || ' ' || BUILTIN.DF(t.custbody_city)
    END AS incoterms,
    BUILTIN.DF(t.employee) AS sales_rep,
    t.foreigntotal * t.exchangerate AS gross_usd,
    (t.foreigntotal - NVL(t.taxtotal, 0)) * t.exchangerate AS net_usd,
    BUILTIN.DF(t.terms) AS terms,
    t.custbody_final_gm_item_freight AS gross_margin,
    t.custbody_items_g_profit_pc_vrq_cost AS gross_margin_pct,
    t.custbody_total_freight_price_usd AS freight_usd,
    t.custbody_total_items_price_usd AS items_usd
FROM transaction t
INNER JOIN CustomerSubsidiaryRelationship csr ON csr.entity = t.entity AND csr.isprimarysub = 'T'
INNER JOIN transactionStatus ts ON ts.id = t.status AND ts.trantype = 'SalesOrd'
LEFT JOIN entityAddressBook eab ON eab.entity = t.entity AND eab.defaultbilling = 'T'
LEFT JOIN EntityAddress ea ON ea.nkey = eab.addressbookaddress
INNER JOIN employee e ON e.id = t.employee
WHERE {' AND '.join(where_clauses)}
ORDER BY
     TO_CHAR(t.trandate, 'YYYY-MM') ASC;
    """
    return sql, params

def get_quotes_by_inside(initial_date: str, final_date: str, inside_sales: str, customer_name: str) -> tuple[str, list[str]]:
    where_clauses = [
        "b.TYPE = 'Estimate'",
        "TO_CHAR(b.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "BUILTIN.DF(b.STATUS) <> 'Quote : Voided'",
        "BUILTIN.DF(b.custbody_evol_idico_services_campo) NOT IN ('ACORD', 'SIEVO')",
        "a.itemtype='InvtPart'",
    ]
    params = [initial_date, final_date]

    if customer_name:
        where_clauses.append("BUILTIN.DF(b.ENTITY) LIKE ?")
        params.append(f"%{customer_name}%")

    if inside_sales:
        where_clauses.append("e.firstname || ' ' || e.lastname LIKE ?")
        params.append(f"%{inside_sales}%")

    sql = f"""

SELECT
    TO_CHAR(b.trandate, 'YYYY-MM-DD') AS CreateDate,
    TO_CHAR(b.duedate, 'YYYY-MM-DD') AS ExpirationDate,
    ts.name AS Status,
    e.firstname || ' ' || e.lastname AS InsideSale,
    b.TRANID AS QuoteNumber,
    BUILTIN.DF(b.ENTITY) AS Customer,
    BUILTIN.DF(a.SUBSIDIARY) AS Subsidiary,
    CASE
        WHEN b.custbody_evol_incoterms IS NOT NULL THEN BUILTIN.DF(b.custbody_evol_incoterms)
        ELSE BUILTIN.DF(b.custbody_inc) || ' ' || BUILTIN.DF(b.custbody_city)
    END AS IncoTerms,
    SUM(a.creditforeignamount*b.EXCHANGERATE) AS Amount,
    b.custbodygross_profit_amt_fr_vc AS GrossMargin,
    b.custbody_gross_profit_percent_final_vc AS GrossMarginPct
FROM transactionline a
LEFT JOIN transaction b ON a.TRANSACTION = b.ID
INNER JOIN employee e ON e.id = b.employee
INNER JOIN transactionStatus ts ON ts.id = b.status AND ts.trantype = 'Estimate'
WHERE {' AND '.join(where_clauses)}
GROUP BY
    TO_CHAR(b.trandate, 'YYYY-MM-DD'),
    TO_CHAR(b.duedate, 'YYYY-MM-DD'),
    ts.name,
    e.firstname || ' ' || e.lastname,
    b.TRANID,
    BUILTIN.DF(b.ENTITY),
    CASE
        WHEN b.custbody_evol_incoterms IS NOT NULL THEN BUILTIN.DF(b.custbody_evol_incoterms)
        ELSE BUILTIN.DF(b.custbody_inc) || ' ' || BUILTIN.DF(b.custbody_city)
    END,
    BUILTIN.DF(a.SUBSIDIARY),
    b.custbodygross_profit_amt_fr_vc,
    b.custbody_gross_profit_percent_final_vc;
"""
    return sql, params

def get_opportunities_data(initial_date: str, final_date: str, inside_sales: str, customer_name: str) -> tuple[str, list[str]]:
    where_clauses = [
        "op.TYPE = 'Opprtnty'",
        "TO_CHAR(op.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "(op.winlossreason <> 21 OR op.winlossreason IS NULL)",
    ]
    params = [initial_date, final_date]

    if customer_name:
        where_clauses.append("BUILTIN.DF(op.entity) LIKE ?")
        params.append(f"%{customer_name}%")

    if inside_sales:
        where_clauses.append("(e.firstname || ' ' || e.lastname) LIKE ?")
        params.append(f"%{inside_sales}%")

    sql = f"""
SELECT
	op.id,
	op.tranid AS op_number,
	TO_CHAR(op.trandate, 'YYYY-MM-DD') AS tran_date,
	TO_CHAR(op.expectedclosedate, 'YYYY-MM-DD') AS expected_close_date,
	BUILTIN.DF(op.entity) AS customer,
	BUILTIN.DF(csr.subsidiary) AS subsidiary,
	ts.name AS status,
	e.firstname || ' ' || e.lastname AS inside_sales
FROM TRANSACTION op
INNER JOIN CustomerSubsidiaryRelationship csr ON csr.entity = op.entity AND csr.isprimarysub = 'T'
INNER JOIN employee e ON e.id = op.employee
INNER JOIN transactionStatus ts ON ts.id = op.status AND ts.trantype = 'Opprtnty'
WHERE {' AND '.join(where_clauses)};
    """
    return sql, params

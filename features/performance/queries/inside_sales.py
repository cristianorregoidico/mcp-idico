def get_op_so_data(initial_date: str, final_date: str) -> tuple[str, list[str]]:
    sql = """
SELECT
	op.tranid AS op_number,
	TO_CHAR(op.trandate, 'YYYY-MM-DD') as op_date,
	ops.name AS op_status,
	e.firstname || ' ' || e.lastname AS inside_sales,
	BUILTIN.DF(op.entity) AS customer,
	q.tranid AS q_number,
	TO_CHAR(q.trandate, 'YYYY-MM-DD') as q_date,
	qs.name AS q_status,
	(q.foreigntotal - NVL(q.taxtotal, 0)) * q.exchangerate AS q_amount,
	so.tranid AS so_number,
	TO_CHAR(so.trandate, 'YYYY-MM-DD') as so_date,
	sos.name AS so_status,
	(so.foreigntotal - NVL(so.taxtotal, 0)) * so.exchangerate AS so_amount
FROM transaction op
INNER JOIN transactionStatus ops ON ops.id = op.status AND ops.trantype = 'Opprtnty'
INNER JOIN employee e ON e.id = op.employee
LEFT JOIN NextTransactionLink ntl ON ntl.previousdoc = op.id AND ntl.linktype = 'OppEst'
LEFT JOIN transaction q ON q.id = ntl.nextdoc AND q.type = 'Estimate'
LEFT JOIN transactionStatus qs ON qs.id = q.status AND qs.trantype = 'Estimate'
LEFT JOIN NextTransactionLink q_to_so ON q_to_so.previousdoc = q.id AND q_to_so.linktype = 'EstInvc'
LEFT JOIN transaction so ON so.id = q_to_so.nextdoc AND so.type = 'SalesOrd'
LEFT JOIN transactionStatus sos ON sos.id = so.status AND sos.trantype = 'SalesOrd'
WHERE op.type = 'Opprtnty'
AND TO_CHAR(op.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?
AND (op.winlossreason <> 21 OR op.winlossreason IS NULL);
    """
    return sql, [initial_date, final_date]

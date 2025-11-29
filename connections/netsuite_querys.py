def get_quotes_by_inside(initial_date: str, final_date: str, inside_sales: str) -> str:
    return f"""

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
WHERE
    b.TYPE = 'Estimate'
    AND TO_CHAR(b.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
    AND BUILTIN.DF(b.STATUS) <> 'Quote : Voided'
    AND BUILTIN.DF(b.custbody_evol_idico_services_campo) NOT IN ('ACORD', 'SIEVO')
    AND a.itemtype='InvtPart'
    AND (
        '{inside_sales}' IS NULL
        OR '{inside_sales}' = ''
        OR e.firstname || ' ' || e.lastname LIKE '%' || '{inside_sales}' || '%'
    )
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

def get_sales_orders_by_inside(initial_date: str, final_date: str, inside_sales: str) -> str:
    return f"""

SELECT
    TO_CHAR(a.trandate, 'YYYY-MM-DD') AS CreateDate,
    ts.name AS Status,
    e.firstname || ' ' || e.lastname AS InsideSale,
    a.TRANID AS SO,
    BUILTIN.DF(a.ENTITY) AS Customer,
    BUILTIN.DF(b.Subsidiary) AS Subsidiary,
    CASE 
        WHEN a.custbody_evol_incoterms IS NOT NULL THEN BUILTIN.DF(a.custbody_evol_incoterms)
        ELSE BUILTIN.DF(a.custbody_inc) || ' ' || BUILTIN.DF(a.custbody_city)
    END AS IncoTerms,
    SUM(b.creditforeignamount*a.EXCHANGERATE) AS Amount
FROM transaction a
LEFT JOIN transactionline b ON a.ID = b.TRANSACTION
INNER JOIN employee e ON e.id = a.employee
INNER JOIN transactionStatus ts ON ts.id = a.status AND ts.trantype = 'SalesOrd'
WHERE
    a.TYPE = 'SalesOrd'
    AND TO_CHAR(a.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
    AND BUILTIN.DF(a.STATUS) NOT IN ('Sales Order : Cancelled','Sales Order : Closed')
    AND BUILTIN.DF(a.custbody_evol_idico_services_campo) NOT IN ('ACORD', 'SIEVO')
    AND b.itemtype='InvtPart'
    AND (
        '{inside_sales}' IS NULL
        OR '{inside_sales}' = ''
        OR e.firstname || ' ' || e.lastname LIKE '%' || '{inside_sales}' || '%'
    )
GROUP BY
    TO_CHAR(a.trandate, 'YYYY-MM-DD'),
    ts.name,
    e.firstname || ' ' || e.lastname,
    a.TRANID,
    BUILTIN.DF(a.ENTITY),
    BUILTIN.DF(b.SUBSIDIARY),
    CASE 
        WHEN a.custbody_evol_incoterms IS NOT NULL THEN BUILTIN.DF(a.custbody_evol_incoterms)
        ELSE BUILTIN.DF(a.custbody_inc) || ' ' || BUILTIN.DF(a.custbody_city)
    END;
    """
    
def get_bookings_by_period(initial_date: str, final_date: str) -> str:
    return f"""
SELECT
    TO_CHAR(t.trandate, 'YYYY-MM') AS period,
    BUILTIN.DF(csr.subsidiary) AS subsidiary,
    BUILTIN.DF(t.entity) AS customer,
    SUM(t.foreigntotal * t.exchangerate) AS gross_usd,
    SUM((t.foreigntotal - NVL(t.taxtotal, 0)) * t.exchangerate) AS net_usd,
    SUM(t.custbody_items_gross_profit_amount_usd) AS gross_margin,
    ROUND(AVG(t.custbody_items_g_profit_pc_vrq_cost), 2) AS gross_margin_pct,
    COUNT(DISTINCT t.id) AS num_transactions,
    COUNT(DISTINCT t.entity) AS num_customers
FROM transaction t
INNER JOIN CustomerSubsidiaryRelationship csr ON csr.entity = t.entity AND csr.isprimarysub = 'T'
INNER JOIN transactionStatus ts ON ts.id = t.status AND ts.trantype = 'SalesOrd'
WHERE TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
  AND csr.subsidiary IN (5, 4, 3)
  AND t.type IN ('SalesOrd')
  AND ts.id NOT IN ('C', 'H', 'A', 'Y')
  AND t.entity NOT IN (37839, 3085, 213418, 2414)
  AND t.employee <> 104334
  AND t.custbody7 = 'F'
GROUP BY
    TO_CHAR(t.trandate, 'YYYY-MM'),
    BUILTIN.DF(csr.subsidiary),
    BUILTIN.DF(t.entity)
ORDER BY
     TO_CHAR(t.trandate, 'YYYY-MM') ASC;
    """

def get_bookings_data(initial_date: str, final_date: str, customer_name: str) -> str:
    return f"""
    SELECT
	t.tranid AS so_number,
	ts.name AS status,
	TO_CHAR(t.trandate, 'YYYY-MM-DD') AS date,
    TO_CHAR(t.trandate, 'YYYY-MM') AS period,
    BUILTIN.DF(csr.subsidiary) AS subsidiary,
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
    t.custbody_items_gross_profit_amount_usd AS gross_margin,
    t.custbody_items_g_profit_pc_vrq_cost AS gross_margin_pct
FROM transaction t
INNER JOIN CustomerSubsidiaryRelationship csr ON csr.entity = t.entity AND csr.isprimarysub = 'T'
INNER JOIN transactionStatus ts ON ts.id = t.status AND ts.trantype = 'SalesOrd'
LEFT JOIN entityAddressBook eab ON eab.entity = t.entity AND eab.defaultbilling = 'T'
LEFT JOIN EntityAddress ea ON ea.nkey = eab.addressbookaddress
WHERE TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
  AND csr.subsidiary IN (5, 4, 3)
  AND t.type  IN ('SalesOrd')
  AND ts.id NOT IN ('C', 'H', 'A', 'Y')
  AND t.entity NOT IN (37839, 3085, 213418,2414)
  AND t.employee <> 104334
  AND t.custbody7 = 'F'
  AND (
        '{customer_name}' IS NULL
        OR '{customer_name}' = ''
        OR BUILTIN.DF(t.entity) LIKE '%' || '{customer_name}' || '%'
    )
ORDER BY
     TO_CHAR(t.trandate, 'YYYY-MM') ASC;
    """

def get_items_quoted_by_customer(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> str:
    return f"""
SELECT 
	BUILTIN.DF(t.entity) AS customer,
	t.tranid AS quote,
	BUILTIN.DF(t.status) AS status,
	t.trandate AS date,
    e.firstname || ' ' || e.lastname AS inside_sales,
	BUILTIN.DF(tl.item) AS item,
    CASE 
        WHEN i.custitem13 IS NULL THEN 'NO DEFINED'
        ELSE BUILTIN.DF(i.custitem13)
    END AS brand,
	CASE 
		WHEN i.class IS NULL THEN 'NO DEFINED'
		ELSE BUILTIN.DF(i.class)
	END AS product_group,
    tl.custcol_evol_selected_vendors AS selected_vendor,
	-tl.quantity AS qty,
	tl.rate AS unit_price
FROM transaction t 
INNER JOIN Customer c ON c.id = t.entity
INNER JOIN transactionLine tl ON tl.transaction = t.id AND tl.itemtype = 'InvtPart'
INNER JOIN item i ON i.id = tl.item
INNER JOIN employee e ON e.id = t.employee
WHERE 
	t.type = 'Estimate'
	AND (
        '{customer_name}' IS NULL
        OR '{customer_name}' = ''
        OR BUILTIN.DF(t.entity) LIKE '%' || '{customer_name}' || '%'
    )
    AND (
        '{inside_sales}' IS NULL
        OR '{inside_sales}' = ''
        OR (e.firstname || ' ' || e.lastname) LIKE '%' || '{inside_sales}' || '%'
    )
	AND TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}';
    """

def get_opportunities_data(initial_date: str, final_date: str, inside_sales: str) -> str:
    return f"""
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
WHERE op.TYPE = 'Opprtnty'
AND (
    '{inside_sales}' IS NULL
    OR '{inside_sales}' = ''
    OR (e.firstname || ' ' || e.lastname) LIKE '%' || '{inside_sales}' || '%'
)
AND TO_CHAR(op.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
AND (op.winlossreason <> 21 OR op.winlossreason IS NULL);
    """

def get_op_so_data(initial_date: str, final_date: str) -> str:
    return f"""
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
AND TO_CHAR(op.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}'
AND (op.winlossreason <> 21 OR op.winlossreason IS NULL);
    """

def get_sold_items_by_period(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> str:
    return f"""
SELECT 
	BUILTIN.DF(t.entity) AS customer,
	t.tranid AS quote,
	ts.name AS status,
	TO_CHAR(t.trandate, 'YYYY-MM-DD') AS date,
    e.firstname || ' ' || e.lastname AS inside_sales,
	BUILTIN.DF(tl.item) AS item,
	SUBSTR(i.purchasedescription, 1, 60) AS item_description,
    CASE 
        WHEN i.custitem13 IS NULL THEN 'NO DEFINED'
        ELSE BUILTIN.DF(i.custitem13)
    END AS brand,
	CASE 
		WHEN i.class IS NULL THEN 'NO DEFINED'
		ELSE BUILTIN.DF(i.class)
	END AS product_group,
    tl.custcol_evol_selected_vendors AS selected_vendor,
	-tl.quantity AS qty,
	tl.rate AS unit_price,
	tl.custcol_evol_vrq_cost AS unit_cost,
	tl.costestimatebase AS estimated_line_cost,
	tl.custcol_gm_percertange AS gross_margin_pct 
FROM transaction t 
INNER JOIN Customer c ON c.id = t.entity
INNER JOIN transactionLine tl ON tl.transaction = t.id AND tl.itemtype = 'InvtPart'
INNER JOIN item i ON i.id = tl.item
INNER JOIN employee e ON e.id = t.employee
INNER JOIN transactionStatus ts ON ts.id = t.status AND ts.trantype = 'SalesOrd'
WHERE 
	t.type = 'SalesOrd'
	AND (
        '{customer_name}' IS NULL
        OR '{customer_name}'= ''
        OR BUILTIN.DF(t.entity) LIKE '%' || '{customer_name}' || '%'
    )
    AND (
        '{inside_sales}' IS NULL
        OR '{inside_sales}' = ''
        OR (e.firstname || ' ' || e.lastname) LIKE '%' || '{inside_sales}' || '%'
    )
	AND TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}';
    """
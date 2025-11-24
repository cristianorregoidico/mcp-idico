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
    SUM(a.creditforeignamount*b.EXCHANGERATE) AS Amount
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
    BUILTIN.DF(a.SUBSIDIARY);
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
    BUILTIN.DF(b.SUBSIDIARY);
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
def get_opportunities_by_inside(initial_date: str, final_date: str, sales_rep: str) -> str:
    return f"""
SELECT 
op.id,
op.tranid AS op_number,
TO_CHAR(op.trandate, 'YYYY-MM-DD') AS tran_date,
BUILTIN.DF(op.entity) AS customer,
BUILTIN.DF(csr.subsidiary) AS subsidiary,
BUILTIN.DF(op.status) AS status,
BUILTIN.DF(op.employee) AS inside_sales
FROM TRANSACTION op
INNER JOIN CustomerSubsidiaryRelationship csr ON csr.entity = op.entity AND csr.isprimarysub = 'T'
WHERE op.TYPE = 'Opprtnty'
AND BUILTIN.DF(op.employee) LIKE '%{sales_rep}%'
AND TO_CHAR(op.trandate, 'YYYY-MM-DD') BETWEEN '{initial_date}' AND '{final_date}';
    """
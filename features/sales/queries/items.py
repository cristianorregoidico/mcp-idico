def get_items_quoted_by_customer(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> tuple[str, list[str]]:
    where_clauses = [
        "t.type = 'Estimate'",
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
    ]
    params = [initial_date, final_date]

    if customer_name:
        where_clauses.append("BUILTIN.DF(t.entity) LIKE ?")
        params.append(f"%{customer_name}%")

    if inside_sales:
        where_clauses.append("(e.firstname || ' ' || e.lastname) LIKE ?")
        params.append(f"%{inside_sales}%")

    sql = f"""
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
WHERE {' AND '.join(where_clauses)};
    """
    return sql, params


def get_sold_items_by_period(initial_date: str, final_date: str, customer_name: str, inside_sales: str) -> tuple[str, list[str]]:
    where_clauses = [
        "t.type = 'SalesOrd'",
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
    ]
    params = [initial_date, final_date]

    if customer_name:
        where_clauses.append("BUILTIN.DF(t.entity) LIKE ?")
        params.append(f"%{customer_name}%")

    if inside_sales:
        where_clauses.append("(e.firstname || ' ' || e.lastname) LIKE ?")
        params.append(f"%{inside_sales}%")

    sql = f"""
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
	tl.custcol_evol_vrq_cost * t.exchangerate AS unit_cost,
	tl.costestimatebase AS estimated_line_cost,
	tl.custcol_gm_percertange AS gross_margin_pct
FROM transaction t
INNER JOIN Customer c ON c.id = t.entity
INNER JOIN transactionLine tl ON tl.transaction = t.id AND tl.itemtype = 'InvtPart'
INNER JOIN item i ON i.id = tl.item
INNER JOIN employee e ON e.id = t.employee
INNER JOIN transactionStatus ts ON ts.id = t.status AND ts.trantype = 'SalesOrd'
WHERE {' AND '.join(where_clauses)};
    """
    return sql, params

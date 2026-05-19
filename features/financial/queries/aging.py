from typing import List, Tuple

_IDICO_ENTITY_IDS = (
    37839,
    3085,
    213418,
    2414,
    355535,
    1066,
    401658,
    101528,
    144291,
    183866,
    185705,
    186223,
)

_AR_TRANSACTION_TYPES = ("CustInvc", "CustCred", "CustPymt")
_AP_TRANSACTION_TYPES = ("VendBill", "VendCred", "VendPymt")

_AR_ACCOUNT_IDS = (2368, 556, 2828, 2723)
_AP_ACCOUNT_IDS = (2366, 2710, 1509, 2916)


def get_aging_data(
    topic: str,
    initial_date: str,
    final_date: str,
    entity_name: str = "",
    subsidiary: str = "",
) -> Tuple[str, List]:
    topic = (topic or "").lower()
    if topic not in ("receivable", "payable"):
        raise ValueError(
            f"Invalid topic '{topic}'. Expected 'receivable' or 'payable'."
        )

    where_clauses: List[str] = [
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "tl.subsidiary IN (3, 4, 5)",
        # Accounting book rule fija: sub 5 -> Primary (1), sub 3 y 4 -> Local GAAP (2)
        "((tl.subsidiary = 5 AND tal.accountingbook = 1)"
        " OR (tl.subsidiary IN (3, 4) AND tal.accountingbook = 2))",
        f"t.entity NOT IN ({', '.join(str(i) for i in _IDICO_ENTITY_IDS)})",
    ]
    params: List = [initial_date, final_date]

    if subsidiary:
        where_clauses.append("tl.subsidiary = ?")
        params.append(subsidiary)

    if topic == "receivable":
        where_clauses.append(
            f't."type" IN ({", ".join(repr(s) for s in _AR_TRANSACTION_TYPES)})'
        )
        where_clauses.append(
            f"tal.account IN ({', '.join(str(a) for a in _AR_ACCOUNT_IDS)})"
        )
        if entity_name:
            where_clauses.append("UPPER(c.companyname) LIKE ?")
            params.append(f"%{entity_name}%")

        select_block = """
            t.id,
            t.tranid AS document_number,
            t."type" AS "type",
            TO_CHAR(t.trandate, 'YYYY-MM-DD') AS trandate,
            TO_CHAR(t.duedate, 'YYYY-MM-DD') AS duedate,
            BUILTIN.DF(t.terms) AS terms,
            t.otherrefnum AS customer_oc,
            BUILTIN.DF(tl.subsidiary) AS subsidiary,
            BUILTIN.DF(t.currency) AS currency,
            t.exchangerate,
            c.entityid AS entity_id,
            c.companyname AS entity_name,
            t.foreignamountpaid,
            BUILTIN.DF(tal.account) AS account,
            BUILTIN.DF(tal.accountingbook) AS accounting_book,
            CASE
                WHEN t."type" = 'CustInvc' THEN tal.amountunpaid
                WHEN t."type" IN ('CustCred', 'CustPymt') THEN tal.paymentamountunused
                ELSE 0
            END AS open_balance
        """
        entity_join = "LEFT JOIN Customer c ON c.id = t.entity"

    else:  # topic == "payable"
        where_clauses.append(
            f't."type" IN ({", ".join(repr(s) for s in _AP_TRANSACTION_TYPES)})'
        )
        where_clauses.append(
            f"tal.account IN ({', '.join(str(a) for a in _AP_ACCOUNT_IDS)})"
        )
        if entity_name:
            where_clauses.append("UPPER(v.companyname) LIKE ?")
            params.append(f"%{entity_name}%")

        select_block = """
            t.id,
            t.tranid AS document_number,
            t."type" AS "type",
            TO_CHAR(t.trandate, 'YYYY-MM-DD') AS trandate,
            TO_CHAR(t.duedate, 'YYYY-MM-DD') AS duedate,
            BUILTIN.DF(t.terms) AS terms,
            BUILTIN.DF(tl.subsidiary) AS subsidiary,
            BUILTIN.DF(t.currency) AS currency,
            t.exchangerate,
            v.entityid AS entity_id,
            v.companyname AS entity_name,
            t.foreignamountpaid,
            BUILTIN.DF(tal.account) AS account,
            BUILTIN.DF(tal.accountingbook) AS accounting_book,
            CASE
                WHEN t."type" = 'VendBill' THEN tal.amountunpaid
                WHEN t."type" IN ('VendCred', 'VendPymt') THEN tal.paymentamountunused
                ELSE 0
            END AS open_balance
        """
        entity_join = "LEFT JOIN Vendor v ON v.id = t.entity"

    sql = f"""
    SELECT {select_block}
    FROM "transaction" t
    {entity_join}
    INNER JOIN TransactionAccountingLine tal
        ON tal."transaction" = t.id
    INNER JOIN transactionLine tl
        ON tl."transaction" = tal."transaction"
       AND tl.id = tal.transactionline
    WHERE {" AND ".join(where_clauses)}
    ORDER BY
        t.trandate DESC,
        t."type";
    """
    return sql, params

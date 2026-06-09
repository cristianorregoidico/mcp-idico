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

_VENDOR_TRANSACTION_TYPES = ("VPrep", "VPrepApp")
_CUSTOMER_TRANSACTION_TYPES = ("CustDep", "DepAppl")

_VENDOR_ACCOUNT_TYPE = "OthCurrAsset"
_CUSTOMER_ACCOUNT_TYPE = "OthCurrLiab"


def get_prepayment_data(
    topic: str,
    initial_date: str,
    final_date: str,
    entity_name: str = "",
    subsidiary: str = "",
) -> Tuple[str, List]:
    topic = (topic or "").lower().strip()

    if topic not in ("vendor", "customer"):
        raise ValueError(f"Invalid topic '{topic}'. Expected 'vendor' or 'customer'.")

    where_clauses: List[str] = [
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "tl.subsidiary IN (3, 4, 5)",
        # Accounting book rule fija:
        # subsidiary 5 -> Primary Accounting Book (1)
        # subsidiary 3 y 4 -> Local GAAP Accounting Book (2)
        "((tl.subsidiary = 5 AND tal.accountingbook = 1)"
        " OR (tl.subsidiary IN (3, 4) AND tal.accountingbook = 2))",
        f"t.entity NOT IN ({', '.join(str(i) for i in _IDICO_ENTITY_IDS)})",
    ]

    params: List = [initial_date, final_date]

    if subsidiary:
        where_clauses.append("tl.subsidiary = ?")
        params.append(subsidiary)

    if topic == "vendor":
        where_clauses.append(
            f't."type" IN ({", ".join(repr(s) for s in _VENDOR_TRANSACTION_TYPES)})'
        )
        where_clauses.append("a.accttype = ?")
        params.append(_VENDOR_ACCOUNT_TYPE)

        if entity_name:
            where_clauses.append(
                "(UPPER(v.companyname) LIKE ? OR UPPER(v.entityid) LIKE ?)"
            )
            entity_filter = f"%{entity_name.upper()}%"
            params.extend([entity_filter, entity_filter])

        entity_join = "LEFT JOIN Vendor v ON v.id = t.entity"

        entity_select_block = """
            v.entityid AS entity_code,
            v.companyname AS entity_name
        """

        status_case = """
            CASE
                WHEN t."type" = 'VPrepApp' AND t.status = 'Y'
                    THEN 'Vendor Prepayment Application : Undefined'

                WHEN t."type" = 'VPrep' AND t.status = 'V'
                    THEN 'Vendor Prepayment : Voided'
                WHEN t."type" = 'VPrep' AND t.status = 'R'
                    THEN 'Vendor Prepayment : Rejected'
                WHEN t."type" = 'VPrep' AND t.status = 'A'
                    THEN 'Vendor Prepayment : Pending Approval'
                WHEN t."type" = 'VPrep' AND t.status = 'E'
                    THEN 'Vendor Prepayment : Partially Applied'
                WHEN t."type" = 'VPrep' AND t.status = 'B'
                    THEN 'Vendor Prepayment : Paid'
                WHEN t."type" = 'VPrep' AND t.status = 'F'
                    THEN 'Vendor Prepayment : Fully Applied'

                ELSE 'Unmapped status'
            END
        """

        movement_case = """
            CASE
                WHEN t."type" = 'VPrep'
                    THEN 'prepayment'
                WHEN t."type" = 'VPrepApp'
                    THEN 'application'
                ELSE 'unknown'
            END
        """

    else:  # topic == "customer"
        where_clauses.append(
            f't."type" IN ({", ".join(repr(s) for s in _CUSTOMER_TRANSACTION_TYPES)})'
        )
        where_clauses.append("a.accttype = ?")
        params.append(_CUSTOMER_ACCOUNT_TYPE)

        if entity_name:
            where_clauses.append(
                "(UPPER(c.companyname) LIKE ? OR UPPER(c.entityid) LIKE ?)"
            )
            entity_filter = f"%{entity_name.upper()}%"
            params.extend([entity_filter, entity_filter])

        entity_join = "LEFT JOIN Customer c ON c.id = t.entity"

        entity_select_block = """
            c.entityid AS entity_code,
            c.companyname AS entity_name
        """

        status_case = """
            CASE
                WHEN t."type" = 'DepAppl' AND t.status = 'Y'
                    THEN 'Deposit Application : Undefined'

                WHEN t."type" = 'CustDep' AND t.status = 'Y'
                    THEN 'Customer Deposit : Undefined'
                WHEN t."type" = 'CustDep' AND t.status = 'D'
                    THEN 'Customer Deposit : Unapproved Payment'
                WHEN t."type" = 'CustDep' AND t.status = 'A'
                    THEN 'Customer Deposit : Not Deposited'
                WHEN t."type" = 'CustDep' AND t.status = 'C'
                    THEN 'Customer Deposit : Fully Applied'
                WHEN t."type" = 'CustDep' AND t.status = 'B'
                    THEN 'Customer Deposit : Deposited'
                WHEN t."type" = 'CustDep' AND t.status = 'R'
                    THEN 'Customer Deposit : Cancelled'

                ELSE 'Unmapped status'
            END
        """

        movement_case = """
            CASE
                WHEN t."type" = 'CustDep'
                    THEN 'prepayment'
                WHEN t."type" = 'DepAppl'
                    THEN 'application'
                ELSE 'unknown'
            END
        """

    sql = f"""
    SELECT
        t.id AS transaction_id,
        t.tranid AS document_number,
        t.transactionnumber,
        t."type" AS transaction_type,
        t.recordtype,
        t.abbrevtype,
        t.status AS status_id,
        {status_case} AS status_name,
        {movement_case} AS movement_type,

        TO_CHAR(t.trandate, 'YYYY-MM-DD') AS trandate,

        t.entity AS entity_id,
        {entity_select_block},

        BUILTIN.DF(t.currency) AS currency,
        t.exchangerate,

        tl.id AS line_id,
        tl.subsidiary AS subsidiary_id,
        BUILTIN.DF(tl.subsidiary) AS subsidiary,
        tl.memo AS line_memo,
        tl.foreignamount AS line_foreignamount,
        tl.netamount AS line_netamount,

        tal.account AS account_id,
        BUILTIN.DF(tal.account) AS account,
        a.accttype AS account_type,

        tal.accountingbook AS accountingbook_id,
        BUILTIN.DF(tal.accountingbook) AS accounting_book,

        tal.amount AS accounting_amount,
        tal.amountpaid AS accounting_amount_paid,
        tal.amountunpaid AS accounting_amount_unpaid,
        tal.paymentamountunused AS payment_amount_unused

    FROM "transaction" t

    {entity_join}

    INNER JOIN TransactionAccountingLine tal
        ON tal."transaction" = t.id

    INNER JOIN transactionLine tl
        ON tl."transaction" = tal."transaction"
       AND tl.id = tal.transactionline

    LEFT JOIN Account a
        ON a.id = tal.account

    WHERE {" AND ".join(where_clauses)}

    ORDER BY
        t.trandate DESC,
        t.id,
        tl.id;
    """

    return sql, params

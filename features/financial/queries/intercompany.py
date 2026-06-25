from typing import List, Tuple

_IDICO_ENTITY_IDS = (
    213418,
    37839,
    355535,
    1066,
)

_OPERATIONAL_TRANSACTION_TYPES = ("Estimate", "SalesOrd", "Opprtnty", "ItemShip")
_FINANCIAL_TRANSACTION_TYPES = ("CustInvc", "CustCred", "CustPymt")


def get_intercompany_data(
    initial_date: str,
    final_date: str,
    entity_name: str = "",
    subsidiary: str = "",
) -> Tuple[str, List]:
    where_clauses: List[str] = [
        "TO_CHAR(t.trandate, 'YYYY-MM-DD') BETWEEN ? AND ?",
        "tl.subsidiary IN (3, 4, 5)",
        # Accounting book rule fija: sub 5 -> Primary (1), sub 3 y 4 -> Local GAAP (2)
        "((tl.subsidiary = 5 AND tal.accountingbook = 1)"
        " OR (tl.subsidiary IN (3, 4) AND tal.accountingbook = 2))",
        # En esta tool los IDs intercompany se incluyen, no se excluyen.
        f"t.entity IN ({', '.join(str(i) for i in _IDICO_ENTITY_IDS)})",
    ]
    params: List = [initial_date, final_date]

    if subsidiary:
        where_clauses.append("tl.subsidiary = ?")
        params.append(subsidiary)

    if entity_name:
        where_clauses.append("UPPER(BUILTIN.DF(t.entity)) LIKE ?")
        params.append(f"%{entity_name}%")

    select_block = f"""
        t.id,
        t.tranid AS document_number,
        t.transactionnumber,
        TO_CHAR(t.trandate, 'YYYY-MM-DD') AS trandate,

        tl.subsidiary AS subsidiary_id,
        BUILTIN.DF(tl.subsidiary) AS subsidiary,

        t.entity AS intercompany_entity_id,
        BUILTIN.DF(t.entity) AS intercompany_entity_name,

        t."type" AS "type",
        t.recordtype,
        t.abbrevtype,
        t.status AS status_id,

        CASE
            WHEN t."type" IN ({", ".join(repr(s) for s in _OPERATIONAL_TRANSACTION_TYPES)})
                THEN 'operational_or_non_balance'
            WHEN t."type" IN ({", ".join(repr(s) for s in _FINANCIAL_TRANSACTION_TYPES)})
                THEN 'financial_candidate'
            ELSE 'other_intercompany_candidate'
        END AS transaction_scope,

        CASE
            WHEN (
                ABS(
                    CASE
                        WHEN tal.amountunpaid IS NOT NULL THEN tal.amountunpaid
                        ELSE 0
                    END
                ) > 0.01
                OR
                ABS(
                    CASE
                        WHEN tal.paymentamountunused IS NOT NULL THEN tal.paymentamountunused
                        ELSE 0
                    END
                ) > 0.01
            )
                THEN 'open_balance_candidate'
            WHEN t."type" IN ({", ".join(repr(s) for s in _OPERATIONAL_TRANSACTION_TYPES)})
                THEN 'operational_or_non_balance'
            ELSE 'movement_only'
        END AS balance_class,

        BUILTIN.DF(t.currency) AS currency,
        t.exchangerate,

        tal.account AS account_id,
        BUILTIN.DF(tal.account) AS account,
        a.accttype AS account_type,

        tal.accountingbook AS accountingbook_id,
        BUILTIN.DF(tal.accountingbook) AS accounting_book,

        tal.amount AS accounting_amount,
        tal.amountpaid AS accounting_amount_paid,
        tal.amountunpaid AS amount_unpaid,
        tal.paymentamountunused AS payment_amount_unused,

        (
            CASE
                WHEN tal.amountunpaid IS NOT NULL THEN tal.amountunpaid
                ELSE 0
            END
            +
            CASE
                WHEN tal.paymentamountunused IS NOT NULL THEN tal.paymentamountunused
                ELSE 0
            END
        ) AS open_balance,

        tl.id AS line_id,
        tl.memo AS line_memo,
        tl.foreignamount AS line_foreignamount,
        tl.netamount AS line_netamount
    """

    sql = f"""
    SELECT {select_block}
    FROM "transaction" t
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
        BUILTIN.DF(tl.subsidiary),
        BUILTIN.DF(t.entity),
        t."type";
    """
    return sql, params

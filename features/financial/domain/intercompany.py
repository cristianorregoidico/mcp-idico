from typing import Any, Dict

import pandas as pd

_AMOUNT_TOLERANCE = 0.01
_TOP_N = 10


def summarize_intercompany_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Resume balance y relacion transaccional intercompany entre subsidiarias.

    df esperado desde queries/intercompany.py con columnas como:
        [
            'id',
            'document_number',
            'transactionnumber',
            'trandate',
            'subsidiary_id',
            'subsidiary',
            'intercompany_entity_id',
            'intercompany_entity_name',
            'type',
            'recordtype',
            'abbrevtype',
            'status_id',
            'transaction_scope',
            'balance_class',
            'currency',
            'exchangerate',
            'account_id',
            'account',
            'account_type',
            'accountingbook_id',
            'accounting_book',
            'accounting_amount',
            'accounting_amount_paid',
            'amount_unpaid',
            'payment_amount_unused',
            'open_balance',
            'line_id',
            'line_memo',
            'line_foreignamount',
            'line_netamount',
        ]

    Nota funcional:
        Esta funcion separa dos lecturas:
        1. Movimiento intercompany: actividad bruta y neta entre subsidiarias.
        2. Saldo abierto intercompany: documentos con amount_unpaid o
           payment_amount_unused.

        No intenta hacer conciliacion contable definitiva documento a documento.
    """
    df = df.copy()

    transaction_col = "transaction_id" if "transaction_id" in df.columns else "id"

    # -----------------------------
    # 1) NORMALIZACION DE TIPOS
    # -----------------------------
    if "trandate" in df.columns:
        df["trandate"] = pd.to_datetime(df["trandate"], errors="coerce")

    numeric_columns = [
        "exchangerate",
        "accounting_amount",
        "accounting_amount_paid",
        "amount_unpaid",
        "payment_amount_unused",
        "open_balance",
        "line_foreignamount",
        "line_netamount",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "accounting_amount" in df.columns:
        df["accounting_amount_abs"] = df["accounting_amount"].abs()
    else:
        df["accounting_amount"] = 0.0
        df["accounting_amount_abs"] = 0.0

    if "line_foreignamount" in df.columns:
        df["line_foreignamount_abs"] = df["line_foreignamount"].abs()
    else:
        df["line_foreignamount"] = 0.0
        df["line_foreignamount_abs"] = 0.0

    if "open_balance" in df.columns:
        df["open_balance_abs"] = df["open_balance"].abs()
    else:
        df["open_balance"] = 0.0
        df["open_balance_abs"] = 0.0

    df_material = df[
        (df["accounting_amount_abs"] > _AMOUNT_TOLERANCE)
        | (df["line_foreignamount_abs"] > _AMOUNT_TOLERANCE)
        | (df["open_balance_abs"] > _AMOUNT_TOLERANCE)
    ].copy()

    df_open = df_material[df_material["open_balance_abs"] > _AMOUNT_TOLERANCE].copy()

    today = pd.Timestamp(pd.Timestamp.today().date())

    # -----------------------------
    # 2) PERIODO
    # -----------------------------
    if "trandate" in df_material.columns and df_material["trandate"].notna().any():
        start_date = df_material["trandate"].min().date().isoformat()
        end_date = df_material["trandate"].max().date().isoformat()
    else:
        start_date = None
        end_date = None

    # -----------------------------
    # 3) TOTALES GENERALES
    # -----------------------------
    row_count = int(len(df_material))

    transaction_count = (
        int(df_material[transaction_col].nunique())
        if transaction_col in df_material.columns
        else int(len(df_material))
    )

    relationship_count = 0
    if {"subsidiary", "intercompany_entity_name"}.issubset(df_material.columns):
        relationship_count = int(
            df_material[["subsidiary", "intercompany_entity_name"]]
            .drop_duplicates()
            .shape[0]
        )

    subsidiary_count = (
        int(df_material["subsidiary_id"].nunique())
        if "subsidiary_id" in df_material.columns
        else 0
    )

    intercompany_entity_count = (
        int(df_material["intercompany_entity_id"].nunique())
        if "intercompany_entity_id" in df_material.columns
        else 0
    )

    currency_count = (
        int(df_material["currency"].nunique())
        if "currency" in df_material.columns
        else 0
    )

    open_balance_transaction_count = (
        int(df_open[transaction_col].nunique())
        if transaction_col in df_open.columns
        else int(len(df_open))
    )

    totals = {
        "row_count": row_count,
        "transaction_count": transaction_count,
        "relationship_count": relationship_count,
        "subsidiary_count": subsidiary_count,
        "intercompany_entity_count": intercompany_entity_count,
        "currency_count": currency_count,
        "net_accounting_movement": float(df_material["accounting_amount"].sum()),
        "gross_accounting_movement": float(df_material["accounting_amount_abs"].sum()),
        "net_foreign_movement": float(df_material["line_foreignamount"].sum()),
        "gross_foreign_movement": float(df_material["line_foreignamount_abs"].sum()),
        "open_balance_sum": float(df_open["open_balance"].sum()),
        "open_balance_abs_sum": float(df_open["open_balance_abs"].sum()),
        "open_balance_transaction_count": open_balance_transaction_count,
        "note": (
            "net_accounting_movement puede netear a cero porque incluye varias "
            "patas contables del asiento. gross_accounting_movement mide volumen "
            "bruto. open_balance_sum usa amount_unpaid + payment_amount_unused."
        ),
    }

    # -----------------------------
    # 4) MATRIZ DE SALDOS ABIERTOS
    # -----------------------------
    open_balance_summary = []
    if {
        "subsidiary",
        "intercompany_entity_name",
        "currency",
        "open_balance",
        "open_balance_abs",
    }.issubset(df_open.columns):
        grp = (
            df_open.groupby(
                ["subsidiary", "intercompany_entity_name", "currency"],
                dropna=False,
            )
            .agg(
                open_balance=("open_balance", "sum"),
                open_balance_abs=("open_balance_abs", "sum"),
                transaction_count=(transaction_col, "nunique"),
                row_count=(transaction_col, "count"),
            )
            .reset_index()
        )

        grp = grp.sort_values("open_balance_abs", ascending=False)

        open_balance_summary = [
            {
                "subsidiary": str(r["subsidiary"])
                if pd.notna(r["subsidiary"])
                else "Unknown",
                "intercompany_entity_name": str(r["intercompany_entity_name"])
                if pd.notna(r["intercompany_entity_name"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "open_balance_abs": float(r["open_balance_abs"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 5) MATRIZ DE MOVIMIENTOS INTERCOMPANY
    # -----------------------------
    movement_summary = []
    if {
        "subsidiary",
        "intercompany_entity_name",
        "currency",
        "accounting_amount",
        "accounting_amount_abs",
        "line_foreignamount",
        "line_foreignamount_abs",
    }.issubset(df_material.columns):
        grp = (
            df_material.groupby(
                ["subsidiary", "intercompany_entity_name", "currency"],
                dropna=False,
            )
            .agg(
                net_accounting_movement=("accounting_amount", "sum"),
                gross_accounting_movement=("accounting_amount_abs", "sum"),
                net_foreign_movement=("line_foreignamount", "sum"),
                gross_foreign_movement=("line_foreignamount_abs", "sum"),
                open_balance=("open_balance", "sum"),
                transaction_count=(transaction_col, "nunique"),
                row_count=(transaction_col, "count"),
            )
            .reset_index()
        )

        grp = grp.sort_values("gross_accounting_movement", ascending=False)

        movement_summary = [
            {
                "subsidiary": str(r["subsidiary"])
                if pd.notna(r["subsidiary"])
                else "Unknown",
                "intercompany_entity_name": str(r["intercompany_entity_name"])
                if pd.notna(r["intercompany_entity_name"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "net_accounting_movement": float(r["net_accounting_movement"]),
                "gross_accounting_movement": float(r["gross_accounting_movement"]),
                "net_foreign_movement": float(r["net_foreign_movement"]),
                "gross_foreign_movement": float(r["gross_foreign_movement"]),
                "open_balance": float(r["open_balance"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 6) DISTRIBUCION POR TRANSACTION SCOPE
    # -----------------------------
    by_transaction_scope = []
    if {
        "transaction_scope",
        "currency",
        "accounting_amount",
        "accounting_amount_abs",
    }.issubset(df_material.columns):
        grp = (
            df_material.groupby(["transaction_scope", "currency"], dropna=False)
            .agg(
                net_accounting_movement=("accounting_amount", "sum"),
                gross_accounting_movement=("accounting_amount_abs", "sum"),
                open_balance=("open_balance", "sum"),
                transaction_count=(transaction_col, "nunique"),
                row_count=(transaction_col, "count"),
            )
            .reset_index()
        )

        by_transaction_scope = [
            {
                "transaction_scope": str(r["transaction_scope"])
                if pd.notna(r["transaction_scope"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "net_accounting_movement": float(r["net_accounting_movement"]),
                "gross_accounting_movement": float(r["gross_accounting_movement"]),
                "open_balance": float(r["open_balance"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 7) DISTRIBUCION POR TIPO DE TRANSACCION
    # -----------------------------
    by_type = []
    if {"type", "currency", "accounting_amount", "accounting_amount_abs"}.issubset(
        df_material.columns
    ):
        grp = (
            df_material.groupby(["type", "currency"], dropna=False)
            .agg(
                net_accounting_movement=("accounting_amount", "sum"),
                gross_accounting_movement=("accounting_amount_abs", "sum"),
                open_balance=("open_balance", "sum"),
                transaction_count=(transaction_col, "nunique"),
                row_count=(transaction_col, "count"),
            )
            .reset_index()
        )

        grp = grp.sort_values("gross_accounting_movement", ascending=False)

        by_type = [
            {
                "type": str(r["type"]) if pd.notna(r["type"]) else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "net_accounting_movement": float(r["net_accounting_movement"]),
                "gross_accounting_movement": float(r["gross_accounting_movement"]),
                "open_balance": float(r["open_balance"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 8) DISTRIBUCION POR ACCOUNT TYPE
    # -----------------------------
    by_account_type = []
    if {
        "account_type",
        "currency",
        "accounting_amount",
        "accounting_amount_abs",
    }.issubset(df_material.columns):
        grp = (
            df_material.groupby(["account_type", "currency"], dropna=False)
            .agg(
                net_accounting_movement=("accounting_amount", "sum"),
                gross_accounting_movement=("accounting_amount_abs", "sum"),
                open_balance=("open_balance", "sum"),
                transaction_count=(transaction_col, "nunique"),
                row_count=(transaction_col, "count"),
            )
            .reset_index()
        )

        grp = grp.sort_values("gross_accounting_movement", ascending=False)

        by_account_type = [
            {
                "account_type": str(r["account_type"])
                if pd.notna(r["account_type"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "net_accounting_movement": float(r["net_accounting_movement"]),
                "gross_accounting_movement": float(r["gross_accounting_movement"]),
                "open_balance": float(r["open_balance"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 9) TOP DOCUMENTOS CON SALDO ABIERTO
    # -----------------------------
    top_open_balance_documents = []
    if {
        transaction_col,
        "document_number",
        "transactionnumber",
        "trandate",
        "subsidiary",
        "intercompany_entity_name",
        "type",
        "currency",
        "account",
        "account_type",
        "open_balance",
        "open_balance_abs",
    }.issubset(df_open.columns):
        grp = (
            df_open.groupby(
                [
                    transaction_col,
                    "document_number",
                    "transactionnumber",
                    "trandate",
                    "subsidiary",
                    "intercompany_entity_name",
                    "type",
                    "currency",
                    "account",
                    "account_type",
                ],
                dropna=False,
            )
            .agg(
                open_balance=("open_balance", "sum"),
                open_balance_abs=("open_balance_abs", "sum"),
            )
            .reset_index()
        )

        grp = grp.sort_values("open_balance_abs", ascending=False).head(_TOP_N)

        top_open_balance_documents = [
            {
                "transaction_id": str(r[transaction_col])
                if pd.notna(r[transaction_col])
                else None,
                "document_number": str(r["document_number"])
                if pd.notna(r["document_number"])
                else None,
                "transactionnumber": str(r["transactionnumber"])
                if pd.notna(r["transactionnumber"])
                else None,
                "trandate": r["trandate"].date().isoformat()
                if pd.notna(r["trandate"])
                else None,
                "subsidiary": str(r["subsidiary"])
                if pd.notna(r["subsidiary"])
                else "Unknown",
                "intercompany_entity_name": str(r["intercompany_entity_name"])
                if pd.notna(r["intercompany_entity_name"])
                else "Unknown",
                "type": str(r["type"]) if pd.notna(r["type"]) else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "account": str(r["account"]) if pd.notna(r["account"]) else "Unknown",
                "account_type": str(r["account_type"])
                if pd.notna(r["account_type"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 10) COMPENSACIONES INVOICE VS CREDIT
    # -----------------------------
    compensation_summary = []
    if {
        "subsidiary",
        "intercompany_entity_name",
        "currency",
        "account",
        "account_type",
        "type",
        "accounting_amount",
        "accounting_amount_abs",
    }.issubset(df_material.columns):
        sub = df_material[df_material["type"].isin(("CustInvc", "CustCred"))].copy()

        if not sub.empty:
            grp = (
                sub.groupby(
                    [
                        "subsidiary",
                        "intercompany_entity_name",
                        "currency",
                        "account",
                        "account_type",
                    ],
                    dropna=False,
                )
                .agg(
                    invoice_amount=(
                        "accounting_amount",
                        lambda s: float(
                            sub.loc[s.index, "accounting_amount"][
                                sub.loc[s.index, "type"] == "CustInvc"
                            ].sum()
                        ),
                    ),
                    credit_amount=(
                        "accounting_amount",
                        lambda s: float(
                            sub.loc[s.index, "accounting_amount"][
                                sub.loc[s.index, "type"] == "CustCred"
                            ].sum()
                        ),
                    ),
                    net_amount=("accounting_amount", "sum"),
                    gross_amount=("accounting_amount_abs", "sum"),
                    transaction_count=(transaction_col, "nunique"),
                )
                .reset_index()
            )

            grp["net_abs"] = grp["net_amount"].abs()
            grp = grp.sort_values(["gross_amount", "net_abs"], ascending=False).head(
                _TOP_N
            )

            compensation_summary = [
                {
                    "subsidiary": str(r["subsidiary"])
                    if pd.notna(r["subsidiary"])
                    else "Unknown",
                    "intercompany_entity_name": str(r["intercompany_entity_name"])
                    if pd.notna(r["intercompany_entity_name"])
                    else "Unknown",
                    "currency": str(r["currency"])
                    if pd.notna(r["currency"])
                    else "Unknown",
                    "account": str(r["account"])
                    if pd.notna(r["account"])
                    else "Unknown",
                    "account_type": str(r["account_type"])
                    if pd.notna(r["account_type"])
                    else "Unknown",
                    "invoice_amount": float(r["invoice_amount"]),
                    "credit_amount": float(r["credit_amount"]),
                    "net_amount": float(r["net_amount"]),
                    "gross_amount": float(r["gross_amount"]),
                    "transaction_count": int(r["transaction_count"]),
                    "interpretation": (
                        "Relacion con alto movimiento bruto y posible compensacion "
                        "entre invoices y credit memos."
                    ),
                }
                for _, r in grp.iterrows()
            ]

    # -----------------------------
    # 11) DATA QUALITY / CONTROL
    # -----------------------------
    unknown_entity_count = 0
    if "intercompany_entity_name" in df_material.columns:
        unknown_entity_count = int(df_material["intercompany_entity_name"].isna().sum())

    operational_row_count = 0
    financial_row_count = 0
    other_row_count = 0

    if "transaction_scope" in df_material.columns:
        operational_row_count = int(
            (df_material["transaction_scope"] == "operational_or_non_balance").sum()
        )
        financial_row_count = int(
            (df_material["transaction_scope"] == "financial_candidate").sum()
        )
        other_row_count = int(
            (df_material["transaction_scope"] == "other_intercompany_candidate").sum()
        )

    data_quality = {
        "raw_row_count": int(len(df)),
        "material_row_count": row_count,
        "transaction_count": transaction_count,
        "open_balance_row_count": int(len(df_open)),
        "unknown_entity_row_count": unknown_entity_count,
        "financial_candidate_row_count": financial_row_count,
        "operational_or_non_balance_row_count": operational_row_count,
        "other_intercompany_candidate_row_count": other_row_count,
        "note": (
            "row_count puede ser mayor que transaction_count porque la fuente "
            "viene a nivel de transactionLine / TransactionAccountingLine."
        ),
    }

    # -----------------------------
    # 12) INTERPRETACION FUNCIONAL
    # -----------------------------
    interpretation = (
        "Esta vista muestra relaciones intercompany usando la relacion "
        "tl.subsidiary -> t.entity. Los movimientos brutos ayudan a entender "
        "actividad o triangulacion entre subsidiarias. Los saldos abiertos se "
        "calculan con amount_unpaid + payment_amount_unused. No debe interpretarse "
        "SUM(accounting_amount) como saldo definitivo, porque al incluir varias "
        "patas contables del asiento el neto puede compensarse a cero."
    )

    # -----------------------------
    # 13) ARMAR OUTPUT FINAL
    # -----------------------------
    output = {
        "topic": "intercompany",
        "as_of_date": today.date().isoformat(),
        "period": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "totals": totals,
        "open_balance_summary": open_balance_summary,
        "movement_summary": movement_summary,
        "by_transaction_scope": by_transaction_scope,
        "by_type": by_type,
        "by_account_type": by_account_type,
        "top_open_balance_documents": top_open_balance_documents,
        "compensation_summary": compensation_summary,
        "data_quality": data_quality,
        "interpretation": interpretation,
        "full_data_reference": "dataset_reference",
    }

    return output

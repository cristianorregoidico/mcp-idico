from typing import Any, Dict

import pandas as pd

_AMOUNT_TOLERANCE = 0.01
_TOP_N = 10


def summarize_prepayment_analysis(df: pd.DataFrame, topic: str) -> Dict[str, Any]:
    """
    Resume anticipos de clientes o proveedores.

    df esperado desde queries/prepayment.py con columnas como:
        [
            'transaction_id',
            'document_number',
            'transactionnumber',
            'transaction_type',
            'recordtype',
            'abbrevtype',
            'status_id',
            'status_name',
            'movement_type',
            'trandate',
            'entity_id',
            'entity_code',
            'entity_name',
            'currency',
            'exchangerate',
            'line_id',
            'subsidiary_id',
            'subsidiary',
            'line_memo',
            'line_foreignamount',
            'line_netamount',
            'account_id',
            'account',
            'account_type',
            'accountingbook_id',
            'accounting_book',
            'accounting_amount',
            'accounting_amount_paid',
            'accounting_amount_unpaid',
            'payment_amount_unused',
        ]

    topic:
        - 'vendor'
        - 'customer'

    Nota funcional:
        Esta función no intenta reconstruir la trazabilidad exacta
        anticipo -> invoice/bill. El objetivo es dar entendimiento
        financiero agregado y transaccional.
    """
    topic = (topic or "").lower().strip()
    if topic not in ("vendor", "customer"):
        raise ValueError(f"Invalid topic '{topic}'. Expected 'vendor' or 'customer'.")

    df = df.copy()

    # -----------------------------
    # 1) NORMALIZACION DE TIPOS
    # -----------------------------
    if "trandate" in df.columns:
        df["trandate"] = pd.to_datetime(df["trandate"], errors="coerce")

    numeric_columns = [
        "line_foreignamount",
        "line_netamount",
        "exchangerate",
        "accounting_amount",
        "accounting_amount_paid",
        "accounting_amount_unpaid",
        "payment_amount_unused",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "line_foreignamount" in df.columns:
        df["movement_amount"] = df["line_foreignamount"]
    else:
        df["movement_amount"] = 0.0

    df["movement_amount_abs"] = df["movement_amount"].abs()

    # Filtramos filas sin valor material.
    df_amount = df[df["movement_amount_abs"] > _AMOUNT_TOLERANCE].copy()

    today = pd.Timestamp(pd.Timestamp.today().date())

    # -----------------------------
    # 2) PERIODO
    # -----------------------------
    if "trandate" in df_amount.columns and df_amount["trandate"].notna().any():
        start_date = df_amount["trandate"].min().date().isoformat()
        end_date = df_amount["trandate"].max().date().isoformat()
    else:
        start_date = None
        end_date = None

    # -----------------------------
    # 3) TOTALES GENERALES
    # -----------------------------
    transaction_count = (
        int(df_amount["transaction_id"].nunique())
        if "transaction_id" in df_amount.columns
        else int(len(df_amount))
    )

    row_count = int(len(df_amount))

    entity_count = (
        int(df_amount["entity_id"].nunique()) if "entity_id" in df_amount.columns else 0
    )

    currency_count = (
        int(df_amount["currency"].nunique()) if "currency" in df_amount.columns else 0
    )

    signed_amount_sum = float(df_amount["movement_amount"].sum())
    absolute_amount_sum = float(df_amount["movement_amount_abs"].sum())

    prepayment_transaction_count = 0
    application_transaction_count = 0

    if {"movement_type", "transaction_id"}.issubset(df_amount.columns):
        prepayment_transaction_count = int(
            df_amount[df_amount["movement_type"] == "prepayment"][
                "transaction_id"
            ].nunique()
        )
        application_transaction_count = int(
            df_amount[df_amount["movement_type"] == "application"][
                "transaction_id"
            ].nunique()
        )

    # -----------------------------
    # 4) DISTRIBUCION POR MOVEMENT TYPE
    # -----------------------------
    by_movement_type = []
    if {"movement_type", "currency", "movement_amount", "movement_amount_abs"}.issubset(
        df_amount.columns
    ):
        grp = (
            df_amount.groupby(["movement_type", "currency"], dropna=False)
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
                entity_count=("entity_id", "nunique"),
            )
            .reset_index()
        )

        by_movement_type = [
            {
                "movement_type": str(r["movement_type"])
                if pd.notna(r["movement_type"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 5) DISTRIBUCION POR CURRENCY
    # -----------------------------
    by_currency = []
    if {"currency", "movement_amount", "movement_amount_abs"}.issubset(
        df_amount.columns
    ):
        grp = (
            df_amount.groupby("currency", dropna=False)
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
                entity_count=("entity_id", "nunique"),
            )
            .reset_index()
        )

        by_currency = [
            {
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 6) DISTRIBUCION POR SUBSIDIARY
    # -----------------------------
    by_subsidiary = []
    if {"subsidiary", "currency", "movement_amount", "movement_amount_abs"}.issubset(
        df_amount.columns
    ):
        grp = (
            df_amount.groupby(["subsidiary", "currency"], dropna=False)
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
                entity_count=("entity_id", "nunique"),
            )
            .reset_index()
        )

        by_subsidiary = [
            {
                "subsidiary": str(r["subsidiary"])
                if pd.notna(r["subsidiary"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 7) DISTRIBUCION POR STATUS
    # -----------------------------
    by_status = []
    if {"status_id", "status_name", "currency", "movement_amount"}.issubset(
        df_amount.columns
    ):
        grp = (
            df_amount.groupby(["status_id", "status_name", "currency"], dropna=False)
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
            )
            .reset_index()
        )

        by_status = [
            {
                "status_id": str(r["status_id"])
                if pd.notna(r["status_id"])
                else "Unknown",
                "status_name": str(r["status_name"])
                if pd.notna(r["status_name"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 8) DISTRIBUCION POR TIPO DE TRANSACCION
    # -----------------------------
    by_transaction_type = []
    if {"transaction_type", "currency", "movement_amount"}.issubset(df_amount.columns):
        grp = (
            df_amount.groupby(["transaction_type", "currency"], dropna=False)
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
            )
            .reset_index()
        )

        by_transaction_type = [
            {
                "transaction_type": str(r["transaction_type"])
                if pd.notna(r["transaction_type"])
                else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 9) TOP ENTITIES
    # -----------------------------
    top_entities = []
    if {
        "entity_id",
        "entity_code",
        "entity_name",
        "currency",
        "movement_amount",
        "movement_amount_abs",
    }.issubset(df_amount.columns):
        grp_entity = (
            df_amount.groupby(
                ["entity_id", "entity_code", "entity_name", "currency"],
                dropna=False,
            )
            .agg(
                signed_amount=("movement_amount", "sum"),
                absolute_amount=("movement_amount_abs", "sum"),
                transaction_count=("transaction_id", "nunique"),
                row_count=("transaction_id", "count"),
                latest_transaction_date=("trandate", "max"),
            )
            .reset_index()
        )

        grp_entity = grp_entity.sort_values("absolute_amount", ascending=False).head(
            _TOP_N
        )

        top_entities = [
            {
                "entity_id": str(r["entity_id"]) if pd.notna(r["entity_id"]) else None,
                "entity_code": str(r["entity_code"])
                if pd.notna(r["entity_code"])
                else None,
                "entity_name": str(r["entity_name"])
                if pd.notna(r["entity_name"])
                else None,
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "signed_amount": float(r["signed_amount"]),
                "absolute_amount": float(r["absolute_amount"]),
                "transaction_count": int(r["transaction_count"]),
                "row_count": int(r["row_count"]),
                "latest_transaction_date": r["latest_transaction_date"]
                .date()
                .isoformat()
                if pd.notna(r["latest_transaction_date"])
                else None,
            }
            for _, r in grp_entity.iterrows()
        ]

    # -----------------------------
    # 10) CONCENTRACION POR MONEDA
    # -----------------------------
    concentration = []
    if {"currency", "entity_id", "movement_amount_abs"}.issubset(df_amount.columns):
        for currency, sub in df_amount.groupby("currency", dropna=False):
            total_amount = float(sub["movement_amount_abs"].sum())
            if total_amount <= 0:
                continue

            by_entity = sub.groupby("entity_id")["movement_amount_abs"].sum()
            top_amount = float(
                by_entity.sort_values(ascending=False).head(_TOP_N).sum()
            )

            concentration.append(
                {
                    "currency": str(currency) if pd.notna(currency) else "Unknown",
                    "top_n": _TOP_N,
                    "top_share_amount": top_amount,
                    "total_amount": total_amount,
                    "top_share_pct": float(top_amount / total_amount),
                }
            )

    # -----------------------------
    # 11) DATA QUALITY / CONTROL
    # -----------------------------
    unmapped_status_count = 0
    if "status_name" in df_amount.columns:
        unmapped_status_count = int(
            (df_amount["status_name"] == "Unmapped status").sum()
        )

    unknown_entity_count = 0
    if "entity_name" in df_amount.columns:
        unknown_entity_count = int(df_amount["entity_name"].isna().sum())

    data_quality = {
        "raw_row_count": int(len(df)),
        "material_row_count": row_count,
        "transaction_count": transaction_count,
        "unmapped_status_row_count": unmapped_status_count,
        "unknown_entity_row_count": unknown_entity_count,
        "note": (
            "row_count puede ser mayor que transaction_count porque la fuente "
            "viene a nivel de transactionLine / TransactionAccountingLine."
        ),
    }

    # -----------------------------
    # 12) INTERPRETACION FUNCIONAL
    # -----------------------------
    if topic == "vendor":
        interpretation = (
            "Vendor prepayments representan dinero pagado por adelantado a proveedores. "
            "Las aplicaciones reducen o consumen ese anticipo. Esta vista ayuda a entender "
            "que proveedores concentran anticipos y como se distribuyen por moneda, "
            "subsidiaria, estado y tipo de movimiento."
        )
    else:
        interpretation = (
            "Customer deposits representan dinero recibido por adelantado de clientes. "
            "Las aplicaciones reducen o consumen ese deposito. Esta vista ayuda a entender "
            "que clientes han pagado anticipos y como se distribuyen por moneda, "
            "subsidiaria, estado y tipo de movimiento."
        )

    # -----------------------------
    # 13) ARMAR OUTPUT FINAL
    # -----------------------------
    output = {
        "topic": topic,
        "as_of_date": today.date().isoformat(),
        "period": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "totals": {
            "row_count": row_count,
            "transaction_count": transaction_count,
            "entity_count": entity_count,
            "currency_count": currency_count,
            "prepayment_transaction_count": prepayment_transaction_count,
            "application_transaction_count": application_transaction_count,
            "signed_amount_sum": signed_amount_sum,
            "absolute_amount_sum": absolute_amount_sum,
            "note": (
                "signed_amount_sum conserva el signo contable de NetSuite. "
                "absolute_amount_sum sirve para medir volumen o concentracion. "
                "No sumar monedas diferentes sin convertir."
            ),
        },
        "by_movement_type": by_movement_type,
        "by_currency": by_currency,
        "by_subsidiary": by_subsidiary,
        "by_status": by_status,
        "by_transaction_type": by_transaction_type,
        "top_entities": top_entities,
        "concentration": concentration,
        "data_quality": data_quality,
        "interpretation": interpretation,
        "full_data_reference": "dataset_reference",
    }

    return output

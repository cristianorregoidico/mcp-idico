from typing import Any, Dict

import pandas as pd


def summarize_receivable_aging(df: pd.DataFrame) -> Dict[str, Any]:
    """
    df: DataFrame con columnas al menos:
        ['id', 'document_number', 'type', 'trandate', 'duedate', 'terms',
         'customer_oc', 'subsidiary', 'currency', 'entity_id',
         'entity_name', 'open_balance']
    """
    df = df.copy()

    # Asegurar tipos
    if "trandate" in df.columns:
        df["trandate"] = pd.to_datetime(df["trandate"], errors="coerce")
    if "duedate" in df.columns:
        df["duedate"] = pd.to_datetime(df["duedate"], errors="coerce")
    if "open_balance" in df.columns:
        df["open_balance"] = pd.to_numeric(df["open_balance"], errors="coerce").fillna(
            0.0
        )

    today = pd.Timestamp(pd.Timestamp.today().date())

    # -----------------------------
    # 1) PERIODO
    # -----------------------------
    if "trandate" in df.columns and df["trandate"].notna().any():
        start_date = df["trandate"].min().date().isoformat()
        end_date = df["trandate"].max().date().isoformat()
    else:
        start_date = None
        end_date = None

    # -----------------------------
    # 2) AGE_DAYS Y termsS
    # -----------------------------
    if "duedate" in df.columns:
        df["age_days"] = (today - df["duedate"]).dt.days
    else:
        df["age_days"] = pd.NA

    def terms(d):
        if pd.isna(d):
            return "no_duedate"
        if d <= 0:
            return "current"
        if d <= 30:
            return "0-30"
        if d <= 60:
            return "31-60"
        if d <= 90:
            return "61-90"
        return "90+"

    df["terms"] = df["age_days"].apply(terms)

    if "open_balance" in df.columns:
        df_open = df[df["open_balance"].abs() > 0.01].copy()
    else:
        df_open = df.copy()

    # -----------------------------
    # 3) TOTALES
    # -----------------------------
    document_count = int(len(df_open))
    entity_count = (
        int(df_open["entity_id"].nunique()) if "entity_id" in df_open.columns else 0
    )
    open_balance_sum = (
        float(df_open["open_balance"].sum())
        if "open_balance" in df_open.columns
        else 0.0
    )

    # -----------------------------
    # 4) DISTRIBUCION POR CURRENCY
    # -----------------------------
    by_currency = []
    if {"currency", "open_balance", "id", "entity_id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby("currency", dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
                entity_count=("entity_id", "nunique"),
            )
            .reset_index()
        )
        by_currency = [
            {
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 5) DISTRIBUCION POR SUBSIDIARY
    # -----------------------------
    by_subsidiary = []
    if {"subsidiary", "currency", "open_balance", "id", "entity_id"}.issubset(
        df_open.columns
    ):
        grp = (
            df_open.groupby(["subsidiary", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
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
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 6) DISTRIBUCION POR TERMS
    # -----------------------------
    terms_order = {
        t: i
        for i, t in enumerate(
            ["current", "0-30", "31-60", "61-90", "90+", "no_duedate"]
        )
    }
    by_terms = []
    if {"terms", "currency", "open_balance", "id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby(["terms", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
            )
            .reset_index()
        )
        grp["__order"] = grp["terms"].map(terms_order).fillna(99)
        grp = grp.sort_values(["__order", "currency"])
        by_terms = [
            {
                "terms": str(r["terms"]),
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 7) DISTRIBUCION POR TIPO DE TRANSACCION
    # -----------------------------
    by_type = []
    if {"type", "currency", "open_balance", "id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby(["type", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
            )
            .reset_index()
        )
        by_type = [
            {
                "type": str(r["type"]) if pd.notna(r["type"]) else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 8) TOP ENTITIES Y CONCENTRACION
    # -----------------------------
    top_n = 10
    top_entities = []
    concentration = []

    if {"entity_id", "entity_name", "currency", "open_balance"}.issubset(
        df_open.columns
    ):
        grp_entity = (
            df_open.groupby(["entity_id", "entity_name", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
                max_age_days=("age_days", "max"),
            )
            .reset_index()
        )
        top_idx = grp_entity["open_balance"].abs().sort_values(ascending=False).index
        grp_top = grp_entity.reindex(top_idx).head(top_n)

        top_entities = [
            {
                "entity_id": str(r["entity_id"]) if pd.notna(r["entity_id"]) else None,
                "entity_name": str(r["entity_name"])
                if pd.notna(r["entity_name"])
                else None,
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "max_age_days": int(r["max_age_days"])
                if pd.notna(r["max_age_days"])
                else None,
            }
            for _, r in grp_top.iterrows()
        ]

        for currency, sub in df_open.groupby("currency", dropna=False):
            total_amount = float(sub["open_balance"].abs().sum())
            if total_amount <= 0:
                continue
            by_entity = sub.groupby("entity_id")["open_balance"].sum().abs()
            top_amount = float(by_entity.sort_values(ascending=False).head(top_n).sum())
            concentration.append(
                {
                    "currency": str(currency) if pd.notna(currency) else "Unknown",
                    "top_n": top_n,
                    "top_share_amount": top_amount,
                    "total_amount": total_amount,
                    "top_share_pct": float(top_amount / total_amount),
                }
            )

    # -----------------------------
    # 9) HOUSEKEEPING (AR: CustPymt y CustCred sin aplicar)
    # -----------------------------
    housekeeping_types = ("CustPymt", "CustCred")
    housekeeping = {"document_count": 0, "by_type": []}
    if "type" in df_open.columns:
        sub = df_open[df_open["type"].isin(housekeeping_types)]
        if not sub.empty:
            grp = (
                sub.groupby(["type", "currency"], dropna=False)
                .agg(
                    open_balance=("open_balance", "sum"),
                    document_count=("id", "count"),
                )
                .reset_index()
            )
            housekeeping = {
                "document_count": int(len(sub)),
                "by_type": [
                    {
                        "type": str(r["type"]),
                        "currency": str(r["currency"])
                        if pd.notna(r["currency"])
                        else "Unknown",
                        "open_balance": float(r["open_balance"]),
                        "document_count": int(r["document_count"]),
                    }
                    for _, r in grp.iterrows()
                ],
                "interpretation": (
                    "Pagos/creditos con saldo abierto. Recuperable internamente "
                    "aplicandolos contra documentos pendientes."
                ),
            }

    # -----------------------------
    # 10) ARMAR JSON FINAL
    # -----------------------------
    output = {
        "topic": "receivable",
        "as_of_date": today.date().isoformat(),
        "period": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "totals": {
            "document_count": document_count,
            "entity_count": entity_count,
            "open_balance_functional_sum": open_balance_sum,
            "note": (
                "open_balance esta en moneda funcional de cada subsidiary; "
                "no sumar entre monedas sin convertir."
            ),
        },
        "by_currency": by_currency,
        "by_subsidiary": by_subsidiary,
        "by_terms": by_terms,
        "by_type": by_type,
        "top_entities": top_entities,
        "concentration": concentration,
        "housekeeping": housekeeping,
        "full_data_reference": "dataset_reference",
    }

    return output


def summarize_payable_aging(df: pd.DataFrame) -> Dict[str, Any]:
    """
    df: DataFrame con columnas al menos:
        ['id', 'document_number', 'type', 'trandate', 'duedate', 'terms',
         'subsidiary', 'currency', 'entity_id', 'entity_name', 'open_balance']
    """
    df = df.copy()

    # Asegurar tipos
    if "trandate" in df.columns:
        df["trandate"] = pd.to_datetime(df["trandate"], errors="coerce")
    if "duedate" in df.columns:
        df["duedate"] = pd.to_datetime(df["duedate"], errors="coerce")
    if "open_balance" in df.columns:
        df["open_balance"] = pd.to_numeric(df["open_balance"], errors="coerce").fillna(
            0.0
        )

    today = pd.Timestamp(pd.Timestamp.today().date())

    # -----------------------------
    # 1) PERIODO
    # -----------------------------
    if "trandate" in df.columns and df["trandate"].notna().any():
        start_date = df["trandate"].min().date().isoformat()
        end_date = df["trandate"].max().date().isoformat()
    else:
        start_date = None
        end_date = None

    # -----------------------------
    # 2) AGE_DAYS Y termsS
    # -----------------------------
    if "duedate" in df.columns:
        df["age_days"] = (today - df["duedate"]).dt.days
    else:
        df["age_days"] = pd.NA

    def terms(d):
        if pd.isna(d):
            return "no_duedate"
        if d <= 0:
            return "current"
        if d <= 30:
            return "0-30"
        if d <= 60:
            return "31-60"
        if d <= 90:
            return "61-90"
        return "90+"

    df["terms"] = df["age_days"].apply(terms)

    if "open_balance" in df.columns:
        df_open = df[df["open_balance"].abs() > 0.01].copy()
    else:
        df_open = df.copy()

    # -----------------------------
    # 3) TOTALES
    # -----------------------------
    document_count = int(len(df_open))
    entity_count = (
        int(df_open["entity_id"].nunique()) if "entity_id" in df_open.columns else 0
    )
    open_balance_sum = (
        float(df_open["open_balance"].sum())
        if "open_balance" in df_open.columns
        else 0.0
    )

    # -----------------------------
    # 4) DISTRIBUCION POR CURRENCY
    # -----------------------------
    by_currency = []
    if {"currency", "open_balance", "id", "entity_id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby("currency", dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
                entity_count=("entity_id", "nunique"),
            )
            .reset_index()
        )
        by_currency = [
            {
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 5) DISTRIBUCION POR SUBSIDIARY
    # -----------------------------
    by_subsidiary = []
    if {"subsidiary", "currency", "open_balance", "id", "entity_id"}.issubset(
        df_open.columns
    ):
        grp = (
            df_open.groupby(["subsidiary", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
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
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "entity_count": int(r["entity_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 6) DISTRIBUCION POR terms
    # -----------------------------
    terms_order = {
        b: i
        for i, b in enumerate(
            ["current", "0-30", "31-60", "61-90", "90+", "no_duedate"]
        )
    }
    by_terms = []
    if {"terms", "currency", "open_balance", "id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby(["terms", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
            )
            .reset_index()
        )
        grp["__order"] = grp["terms"].map(terms_order).fillna(99)
        grp = grp.sort_values(["__order", "currency"])
        by_terms = [
            {
                "terms": str(r["terms"]),
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 7) DISTRIBUCION POR TIPO DE TRANSACCION
    # -----------------------------
    by_type = []
    if {"type", "currency", "open_balance", "id"}.issubset(df_open.columns):
        grp = (
            df_open.groupby(["type", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
            )
            .reset_index()
        )
        by_type = [
            {
                "type": str(r["type"]) if pd.notna(r["type"]) else "Unknown",
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
            }
            for _, r in grp.iterrows()
        ]

    # -----------------------------
    # 8) TOP ENTITIES Y CONCENTRACION
    # -----------------------------
    top_n = 10
    top_entities = []
    concentration = []

    if {"entity_id", "entity_name", "currency", "open_balance"}.issubset(
        df_open.columns
    ):
        grp_entity = (
            df_open.groupby(["entity_id", "entity_name", "currency"], dropna=False)
            .agg(
                open_balance=("open_balance", "sum"),
                document_count=("id", "count"),
                max_age_days=("age_days", "max"),
            )
            .reset_index()
        )
        top_idx = grp_entity["open_balance"].abs().sort_values(ascending=False).index
        grp_top = grp_entity.reindex(top_idx).head(top_n)

        top_entities = [
            {
                "entity_id": str(r["entity_id"]) if pd.notna(r["entity_id"]) else None,
                "entity_name": str(r["entity_name"])
                if pd.notna(r["entity_name"])
                else None,
                "currency": str(r["currency"])
                if pd.notna(r["currency"])
                else "Unknown",
                "open_balance": float(r["open_balance"]),
                "document_count": int(r["document_count"]),
                "max_age_days": int(r["max_age_days"])
                if pd.notna(r["max_age_days"])
                else None,
            }
            for _, r in grp_top.iterrows()
        ]

        for currency, sub in df_open.groupby("currency", dropna=False):
            total_amount = float(sub["open_balance"].abs().sum())
            if total_amount <= 0:
                continue
            by_entity = sub.groupby("entity_id")["open_balance"].sum().abs()
            top_amount = float(by_entity.sort_values(ascending=False).head(top_n).sum())
            concentration.append(
                {
                    "currency": str(currency) if pd.notna(currency) else "Unknown",
                    "top_n": top_n,
                    "top_share_amount": top_amount,
                    "total_amount": total_amount,
                    "top_share_pct": float(top_amount / total_amount),
                }
            )

    # -----------------------------
    # 9) HOUSEKEEPING (AP: VendPymt y VendCred sin aplicar)
    # -----------------------------
    housekeeping_types = ("VendPymt", "VendCred")
    housekeeping = {"document_count": 0, "by_type": []}
    if "type" in df_open.columns:
        sub = df_open[df_open["type"].isin(housekeeping_types)]
        if not sub.empty:
            grp = (
                sub.groupby(["type", "currency"], dropna=False)
                .agg(
                    open_balance=("open_balance", "sum"),
                    document_count=("id", "count"),
                )
                .reset_index()
            )
            housekeeping = {
                "document_count": int(len(sub)),
                "by_type": [
                    {
                        "type": str(r["type"]),
                        "currency": str(r["currency"])
                        if pd.notna(r["currency"])
                        else "Unknown",
                        "open_balance": float(r["open_balance"]),
                        "document_count": int(r["document_count"]),
                    }
                    for _, r in grp.iterrows()
                ],
                "interpretation": (
                    "Pagos/creditos con saldo abierto. Recuperable internamente "
                    "aplicandolos contra documentos pendientes."
                ),
            }

    # -----------------------------
    # 10) ARMAR JSON FINAL
    # -----------------------------
    output = {
        "topic": "payable",
        "as_of_date": today.date().isoformat(),
        "period": {
            "start_date": start_date,
            "end_date": end_date,
        },
        "totals": {
            "document_count": document_count,
            "entity_count": entity_count,
            "open_balance_functional_sum": open_balance_sum,
            "note": (
                "open_balance esta en moneda funcional de cada subsidiary; "
                "no sumar entre monedas sin convertir."
            ),
        },
        "by_currency": by_currency,
        "by_subsidiary": by_subsidiary,
        "by_terms": by_terms,
        "by_type": by_type,
        "top_entities": top_entities,
        "concentration": concentration,
        "housekeeping": housekeeping,
        "full_data_reference": "dataset_reference",
    }

    return output

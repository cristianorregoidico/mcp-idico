import pandas as pd
import numpy as np

def opportunity_summary(df: pd.DataFrame) -> dict:
    """
    df: DataFrame con columnas:
        ['id','op_number','tran_date','expected_close_date',
         'customer','subsidiary','status','inside_sales']
    """

    # Asegurar tipo fecha
    df["tran_date"] = pd.to_datetime(df["tran_date"], errors="coerce")
    df_valid = df.dropna(subset=["tran_date"])

    if df_valid.empty:
        return {
            "period": {
                "start_date": None,
                "end_date": None,
            },
            "overview": {
                "total_opportunities": 0,
                "total_unique_customers": 0,
                "customer_participation": [],
            },
            "distribution": {
                "inside_sales": [],
                "status": [],
            },
            "overdue_in_progress": [],
            "low_performance_indicators": {
                "daily": [],
                "weekly": [],
                "monthly": [],
            },
            "full_data_reference": "dataset_reference",
        }

    # -----------------------------
    # 1) PERIODO
    # -----------------------------
    start_ts = df_valid["tran_date"].min()
    end_ts   = df_valid["tran_date"].max()

    start_date_obj = start_ts.date()
    end_date_obj   = end_ts.date()

    start_date = start_date_obj.isoformat()
    end_date   = end_date_obj.isoformat()

    today = pd.Timestamp.today().date()

    # ¿Misma semana ISO?
    start_iso = start_date_obj.isocalendar()
    end_iso   = end_date_obj.isocalendar()
    same_week = (start_iso.year == end_iso.year) and (start_iso.week == end_iso.week)

    # -----------------------------
    # 2) PERFORMANCE DE OPORTUNIDADES
    #    (siempre calculamos todo, pero mostramos según regla)
    # -----------------------------

    # --- Por día ---
    daily_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.date])
        .size()
        .reset_index(name="count")
    )
    daily_counts.columns = ["inside_sales", "date", "count"]
    daily_counts["date"] = daily_counts["date"].astype(str)
    low_daily_all = daily_counts[daily_counts["count"] < 4]

    # --- Por semana ---
    weekly_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.to_period("W")])
        .size()
        .reset_index(name="count")
    )
    weekly_counts.columns = ["inside_sales", "week", "count"]
    weekly_counts["week"] = weekly_counts["week"].astype(str)
    low_weekly_all = weekly_counts[weekly_counts["count"] < 15]

    # --- Por mes ---
    monthly_counts = (
        df_valid
        .groupby(["inside_sales", df_valid["tran_date"].dt.to_period("M")])
        .size()
        .reset_index(name="count")
    )
    monthly_counts.columns = ["inside_sales", "month", "count"]
    monthly_counts["month"] = monthly_counts["month"].astype(str)
    low_monthly_all = monthly_counts[monthly_counts["count"] < 50]

    # -----------------------------
    # 3) REGLA DE QUÉ MOSTRAR
    # -----------------------------
    # Caso 1: mismo día y es hoy -> solo daily
    if (start_date_obj == end_date_obj) and (start_date_obj == today):
        low_daily   = low_daily_all
        low_weekly  = pd.DataFrame(columns=["inside_sales", "week", "count"])
        low_monthly = pd.DataFrame(columns=["inside_sales", "month", "count"])

    # Caso 2: fechas distintas pero misma semana -> solo weekly
    elif (start_date_obj != end_date_obj) and same_week:
        low_daily   = pd.DataFrame(columns=["inside_sales", "date", "count"])
        low_weekly  = low_weekly_all
        low_monthly = pd.DataFrame(columns=["inside_sales", "month", "count"])

    # Caso 3: resto -> trabajar por mes (mismo mes o meses distintos)
    else:
        low_daily   = pd.DataFrame(columns=["inside_sales", "date", "count"])
        low_weekly  = pd.DataFrame(columns=["inside_sales", "week", "count"])
        low_monthly = low_monthly_all
        # Aquí, si hay varios meses, saldrán como:
        # month: "2025-10", "2025-11", etc.

    low_performance_indicators = {
        "daily":   low_daily.to_dict(orient="records"),
        "weekly":  low_weekly.to_dict(orient="records"),
        "monthly": low_monthly.to_dict(orient="records"),
    }

     # Distribución por cliente
    dist_customer = (
        df.groupby("customer").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Distribución por inside
    dist_inside = (
        df.groupby("inside_sales").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Distribución por estado
    dist_status = (
        df.groupby("status").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .to_dict(orient="records")
    )

    # Oportunidades con 2+ días sin cotización
    df_valid = df.copy()
    df_valid["tran_date"] = pd.to_datetime(df_valid["tran_date"], errors="coerce")

    today = pd.Timestamp.today().normalize()
    df_valid["days_open"] = (today - df_valid["tran_date"]).dt.days

    overdue = df_valid[
        (df_valid["status"] == "In Progress") &
        (df_valid["days_open"] >= 2)
    ].copy()

    overdue["tran_date"] = overdue["tran_date"].dt.date.astype(str)

    overdue_list = overdue.head(10).to_dict(orient="records")
    
    total_opportunities = len(df_valid)
    total_customers = df_valid["customer"].nunique()

    customer_counts = (
        df_valid.groupby("customer")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    customer_counts["participation_pct"] = (
        customer_counts["count"] / total_opportunities * 100
    ).round(2)
    customer_participation_top10 = customer_counts.head(10).to_dict(orient="records")
    summary = {
        "total_opportunities": total_opportunities,
        "total_unique_customers": total_customers,
        "customer_participation": customer_participation_top10
    }
    # -----------------------------
    # 8) ARMAR JSON FINAL
    # -----------------------------
    output = {
            "period": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "overview": summary,
            "distribution": {
                "inside_sales": dist_inside,
                "status": dist_status
            },
            "overdue_in_progress": overdue_list,
            "low_performance_indicators": low_performance_indicators,
            "full_data_reference": "dataset_reference"
    }

    return output

import pandas as pd
import numpy as np
import json
from typing import Dict, Any

def on_time_delivery_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calcula el % de entrega a tiempo por mes (según if_create_date),
    usando:
      - Numerador: conteo único de item_name_so con delivery_status == "On Time"
      - Denominador: conteo único total de item_name_so en ese mes

    Devuelve un dict listo para JSON con shape:

    {
      "on_time_delivery": {
        "by_month": [
          {
            "month": "2025-11",
            "total_items_delivery": 10,
            "items_on_time": 8,
            "on_time_pct": 0.8
          }
        ],
        "overall": {
          "unique_items_total": 12,
          "unique_items_on_time": 9,
          "on_time_pct": 0.75
        }
      }
    }
    """

    df = df.copy()

    # Tratar "" como NaN para item_name_so
    df["item_name_so"] = df["item_name_so"].replace("", np.nan)

    # Parsear fecha
    df["if_create_date"] = pd.to_datetime(df["if_create_date"], errors="coerce")

    # Mes (periodo)
    df["if_month"] = df["if_create_date"].dt.to_period("M")

    # Filtrar filas con fecha válida y item no nulo
    df_valid = df.dropna(subset=["if_month", "item_name_so"])

    # -------------------------
    # 1) RESUMEN POR MES
    # -------------------------

    # Denominador: total de registros por mes (no únicos)
    total_counts = (
        df_valid.groupby("if_month")["item_name_so"]
        .count()
        .rename("total_items_delivery")
    )

    # Numerador: total de registros On Time por mes (no únicos)
    on_time_counts = (
        df_valid.loc[df_valid["delivery_status"] == "On Time"]
        .groupby("if_month")["item_name_so"]
        .count()
        .rename("items_on_time")
    )

    # Número de SO distintos en el dataset filtrado
    total_so_delivery = int(df_valid["so_doc_number"].nunique())

    # Unir y calcular %
    result = (
        pd.concat([total_counts, on_time_counts], axis=1)
        .fillna(0)
        .reset_index()
    )

    # Asegurar tipos numéricos
    result["total_items_delivery"] = result["total_items_delivery"].astype(int)
    result["items_on_time"] = result["items_on_time"].astype(int)

    # Porcentaje
    result["on_time_pct"] = np.where(
        result["total_items_delivery"] > 0,
        result["items_on_time"] / result["total_items_delivery"],
        0.0
    )

    # Convertir if_month a string "YYYY-MM"
    result["month"] = result["if_month"].astype(str)

    # Seleccionar columnas finales para by_month
    result_final = result[["month", "total_items_delivery", "items_on_time", "on_time_pct"]]

    by_month = result_final.to_dict(orient="records")

    # -------------------------
    # 2) RESUMEN OVERALL (totales, no únicos)
    # -------------------------

    overall_total = int(len(df_valid))
    overall_on_time = int((df_valid["delivery_status"] == "On Time").sum())

    if overall_total > 0:
        overall_on_time_pct = overall_on_time / overall_total
    else:
        overall_on_time_pct = 0.0

    overall = {
        "total_items_delivery": overall_total,
        "items_on_time": overall_on_time,
        "on_time_pct": float(overall_on_time_pct)
    }

    # -------------------------
    # 3) DISTRIBUCIÓN DE po_status (clave = po_status, valor = count de po_doc_number únicos)
    # -------------------------

    po_status_distribution: Dict[str, int] = {}

    if {"po_doc_number", "po_status"}.issubset(df.columns):
        df_po = df.copy()
        df_po["po_status_clean"] = df_po["po_status"].fillna("None")

        # Agrupar por estado y contar po_doc_number únicos
        status_grp = (
            df_po.groupby("po_status_clean", dropna=False)["po_doc_number"]
            .nunique()
            .reset_index(name="count")
        )

        for _, row in status_grp.iterrows():
            status_key = str(row["po_status_clean"])
            po_status_distribution[status_key] = int(row["count"])

    # -------------------------
    # 4) OUTPUT FINAL
    # -------------------------
    output = {
        "on_time_delivery": {
            "by_month": by_month,
            "overall": overall
        },
        "total_so_delivery": total_so_delivery,
        "po_status_distribution": po_status_distribution,
        "so_details": None
    }

    return output
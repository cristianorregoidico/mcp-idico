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
  
def build_imports_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    df: DataFrame con columnas:
    ['dia', 'mes', 'ano', 'importador', 'partida_arancelaria',
     'descripcion_arancelaria', 'producto', 'pais_de_origen',
     'pais_de_adquisicion', 'via_de_transporte', 'transportador',
     'proveedor', 'unidad_de_medida', 'amount_us_cif', 'peso_neto',
     'cantidad', 'amount_us_fob', 'pais', 'marca', 'incoterm']
    """

    df = df.copy()

    # Asegurar numéricos
    for col in ["amount_us_fob", "amount_us_cif"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # --------------------------
    # RESUMEN GENERAL
    # --------------------------
    total_fob = float(df["amount_us_fob"].sum())
    total_cif = float(df["amount_us_cif"].sum())
    unique_brands = int(df["marca"].nunique(dropna=True))
    unique_vendors = int(df["proveedor"].nunique(dropna=True))
    years = sorted([int(y) for y in df["ano"].dropna().unique().tolist()])

    # amounts por año
    year_agg = (
        df.groupby("ano")[["amount_us_fob", "amount_us_cif"]]
        .sum()
        .reset_index()
    )

    amounts_by_year = {
        int(row["ano"]): {
            "amount_us_fob": float(row["amount_us_fob"]),
            "amount_us_cif": float(row["amount_us_cif"])
        }
        for _, row in year_agg.iterrows()
    }

    summary = {
        "years": years,
        "amounts_by_year": amounts_by_year,
        "total_amount_us_fob": total_fob,
        "total_amount_us_cif": total_cif,
        "unique_brands": unique_brands,
        "unique_vendors": unique_vendors,
        "total_records": int(len(df))
    }

    # Helper para convertir a lista de dicts con sumatorias
    def brand_distribution_for_year(df_year: pd.DataFrame, top_n: int = 15):
        if "marca" not in df_year.columns:
            return []
        grp = (
            df_year.groupby("marca", dropna=False)[["amount_us_fob", "amount_us_cif"]]
            .sum()
            .reset_index()
        )
        # Ordenar por monto total (FOB + CIF) desc
        grp["total_amount"] = grp["amount_us_fob"] + grp["amount_us_cif"]
        grp = grp.sort_values("total_amount", ascending=False).head(top_n)

        records = []
        for _, row in grp.iterrows():
            records.append({
                "brand": None if pd.isna(row["marca"]) else str(row["marca"]),
                "amount_us_fob": float(row["amount_us_fob"]),
                "amount_us_cif": float(row["amount_us_cif"])
            })
        return records

    def arancel_distribution_for_year(df_year: pd.DataFrame, top_n: int = 15):
        if "descripcion_arancelaria" not in df_year.columns:
            return []
        grp = (
            df_year.groupby("descripcion_arancelaria", dropna=False)[["amount_us_fob", "amount_us_cif"]]
            .sum()
            .reset_index()
        )
        grp["total_amount"] = grp["amount_us_fob"] + grp["amount_us_cif"]
        grp = grp.sort_values("total_amount", ascending=False).head(top_n)

        records = []
        for _, row in grp.iterrows():
            records.append({
                "descripcion_arancelaria": None if pd.isna(row["descripcion_arancelaria"]) else str(row["descripcion_arancelaria"]),
                "amount_us_fob": float(row["amount_us_fob"]),
                "amount_us_cif": float(row["amount_us_cif"])
            })
        return records

    def incoterm_distribution_for_year(df_year: pd.DataFrame):
        if "incoterm" not in df_year.columns:
            return []
        grp = (
            df_year.groupby("incoterm", dropna=False)[["amount_us_fob", "amount_us_cif"]]
            .sum()
            .reset_index()
        )

        records = []
        for _, row in grp.iterrows():
            records.append({
                "incoterm": None if pd.isna(row["incoterm"]) else str(row["incoterm"]),
                "amount_us_fob": float(row["amount_us_fob"]),
                "amount_us_cif": float(row["amount_us_cif"])
            })
        return records

    def vendor_distribution_for_year(df_year: pd.DataFrame, top_n: int = 15):
      if "proveedor" not in df_year.columns:
          return []
      grp = (
          df_year.groupby("proveedor", dropna=False)[["amount_us_fob", "amount_us_cif"]]
          .sum()
          .reset_index()
      )
      grp["total_amount"] = grp["amount_us_fob"] + grp["amount_us_cif"]
      grp = grp.sort_values("total_amount", ascending=False).head(top_n)

      records = []
      for _, row in grp.iterrows():
          proveedor = None if pd.isna(row["proveedor"]) else str(row["proveedor"])
          records.append({
              "proveedor": proveedor,
              "amount_us_fob": float(row["amount_us_fob"]),
              "amount_us_cif": float(row["amount_us_cif"])
          })
      return records


    # --------------------------
    # BLOQUES POR AÑO
    # --------------------------
    output: Dict[str, Any] = {
        "summary": summary
    }

    for year in years:
        df_year = df[df["ano"] == year].copy()

        year_key = f"year_{year}"
        output[year_key] = {
            "brand_distribution": brand_distribution_for_year(df_year),
            "arancel_distribution": arancel_distribution_for_year(df_year),
            "incoterm_distribution": incoterm_distribution_for_year(df_year),
            "vendor_distribution": vendor_distribution_for_year(df_year),
        }

    return output
from typing import Any, Dict

import pandas as pd


def build_imports_summary(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()
    for col in ["amount_us_fob", "amount_us_cif"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    total_fob = float(df["amount_us_fob"].sum())
    total_cif = float(df["amount_us_cif"].sum())
    unique_brands = int(df["marca"].nunique(dropna=True))
    unique_vendors = int(df["proveedor"].nunique(dropna=True))
    years = sorted([int(y) for y in df["ano"].dropna().unique().tolist()])

    year_agg = df.groupby("ano")[["amount_us_fob", "amount_us_cif"]].sum().reset_index()
    amounts_by_year = {
        int(row["ano"]): {"amount_us_fob": float(row["amount_us_fob"]), "amount_us_cif": float(row["amount_us_cif"])}
        for _, row in year_agg.iterrows()
    }

    summary = {
        "years": years,
        "amounts_by_year": amounts_by_year,
        "total_amount_us_fob": total_fob,
        "total_amount_us_cif": total_cif,
        "unique_brands": unique_brands,
        "unique_vendors": unique_vendors,
        "total_records": int(len(df)),
    }

    def _distribution(df_year: pd.DataFrame, by_col: str, key: str, top_n: int = 15):
        if by_col not in df_year.columns:
            return []
        grp = df_year.groupby(by_col, dropna=False)[["amount_us_fob", "amount_us_cif"]].sum().reset_index()
        grp["total_amount"] = grp["amount_us_fob"] + grp["amount_us_cif"]
        if top_n:
            grp = grp.sort_values("total_amount", ascending=False).head(top_n)
        return [
            {
                key: None if pd.isna(row[by_col]) else str(row[by_col]),
                "amount_us_fob": float(row["amount_us_fob"]),
                "amount_us_cif": float(row["amount_us_cif"]),
            }
            for _, row in grp.iterrows()
        ]

    output: Dict[str, Any] = {"summary": summary}
    for year in years:
        df_year = df[df["ano"] == year].copy()
        output[f"year_{year}"] = {
            "brand_distribution": _distribution(df_year, "marca", "brand", top_n=15),
            "arancel_distribution": _distribution(df_year, "descripcion_arancelaria", "descripcion_arancelaria", top_n=15),
            "incoterm_distribution": _distribution(df_year, "incoterm", "incoterm", top_n=0),
            "vendor_distribution": _distribution(df_year, "proveedor", "proveedor", top_n=15),
        }
    return output

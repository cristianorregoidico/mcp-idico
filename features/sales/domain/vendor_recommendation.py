import pandas as pd
from typing import Dict, Any

def analize_hr_desviado(df_cus_brand: pd.DataFrame, df_country_brand: pd.DataFrame) -> Dict[str, Any]:
    """
    Divide el dataframe por año y ordena cada subset por probabilidad descendente.
    
    Returns:
        df_2025, df_2024
    """
    
    df_customer_2025 = (
        df_cus_brand[df_cus_brand["year"] == 2025]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_customer_2024 = (
        df_cus_brand[df_cus_brand["year"] == 2024]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_country_2025 = (
        df_country_brand[df_country_brand["year"] == 2025]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    
    df_country_2024 = (
        df_country_brand[df_country_brand["year"] == 2024]
        .sort_values(by="probabilidad", ascending=False)
        .reset_index(drop=True)
        .to_dict(orient="records")
    )
    result = {
        "vendors_by_customer_brand": {
            "current": {
                "label": "Based on current real time data",
                "data": df_customer_2025 if len(df_customer_2025) > 0 else "No recent data available this client" 
            },
            "old": {
                "label": "Based on data before to 2025",
                "data": df_customer_2024 if len(df_customer_2024) > 0 else "No historical data available this client"
            }
        },
        "vendors_by_country_brand": {
            "current": {
                "label": "Based on current real time data",
                "data": df_country_2025 if len(df_country_2025) > 0 else "No recent data available this country"
            },
            "old": {
                "label": "Based on data before to 2025",
                "data": df_country_2024 if len(df_country_2024) > 0 else "No historical data available this country"
            }
        }
    }

    return result

import json
from typing import List, Tuple, Any, Optional
import datetime


def save_result_to_json(
    columns: List[str],
    rows: List[Tuple[Any, ...]],
    description: str,
    name: str = "sales_dataset.json",
    selected_columns: Optional[List[str]] = None,
) -> None:
    """
    Guarda los datos de una consulta (columns + rows) en un archivo JSON
    con la estructura:
    {
      "data_set_description": "",
      "columns": [],
      "rows": [
        []
      ]
    }

    - columns: lista de columnas originales devueltas por la BD.
    - rows: lista de tuplas con los valores, en el mismo orden que `columns`.
    - selected_columns: columnas que quieres incluir (en el orden que las pongas).
      Si es None, se usan todas las columnas.
    """

    # Si no se especifican columnas, usamos todas
    if selected_columns is None:
        filtered_columns = list(columns)
        filtered_rows = [list(row) for row in rows]
    else:
        # Mapear nombre de columna -> índice en la fila original
        col_index = {name: i for i, name in enumerate(columns)}

        # Validar que todas las columnas seleccionadas existen
        missing = [c for c in selected_columns if c not in col_index]
        if missing:
            raise ValueError(f"Estas columnas no existen en el resultado: {missing}")

        filtered_columns = list(selected_columns)

        # Re-construir cada fila solo con las columnas seleccionadas
        filtered_rows = [
            [row[col_index[col_name]] for col_name in selected_columns]
            for row in rows
        ]

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{name}.json"
    data = {
        "data_set_description": description,
        "columns": filtered_columns,
        "rows": filtered_rows,
    }

    with open("data/"+filename, "w", encoding="utf-8") as f:
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return json.JSONEncoder.default(self, obj)
        json.dump(data, f, cls=DateEncoder, ensure_ascii=False, indent=2)
    
    return filename

import json
import pandas as pd
from typing import Tuple


def load_dataset_from_json(filename: str) -> Tuple[pd.DataFrame, str]:
    """
    Lee un archivo JSON con estructura:
    {
      "data_set_description": "",
      "columns": [],
      "rows": [
        []
      ]
    }

    y devuelve:
      - df: pandas.DataFrame con los datos
      - description: cadena con la descripción del dataset
    """
    with open("data/"+filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    # description = data.get("data_set_description", "")
    # columns = data["columns"]
    # rows = data["rows"]
    #df = pd.DataFrame(rows, columns=columns)
    
    return data
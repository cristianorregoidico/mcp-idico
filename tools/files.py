from typing import Any, Dict, List
from utils.json_df import load_dataset_from_json
from utils.envelope import build_tool_response
from pathlib import Path
from fastmcp.utilities.types import File

DATA_DIR = Path("data").resolve()

def get_dataset(data_set_reference: str) -> Dict[str, Any]:
    """Retrieve a dataset previously saved from an user query.

    Args:
        data_set_reference: The filename or identifier of the saved dataset.
    Returns:
        Dict[str, Any]: The dataset loaded from JSON.
    """
    

    dataset = load_dataset_from_json(data_set_reference)
    return dataset

def get_excel_file(file_name: str) -> File:
    """
    Retrieve an Excel file previously saved from an user query.
    
    Use this tool when user requests an Excel file generated from a dataset.
    
    Args:
        file_name: The filename of the saved Excel file.
    Returns:
        File: The Excel file as a FastMCP File object.
    """
    # Normaliza a "solo nombre de archivo" (evita rutas)
    safe_name = Path(file_name).name
    print("safe_name", safe_name)
    path = (DATA_DIR / safe_name).resolve()
    print("path", path)

   # Anti path-traversal: obliga a que esté dentro de DATA_DIR
    if not path.is_relative_to(DATA_DIR):
        raise ValueError("Nombre de archivo inválido")

    if not path.exists():
        raise FileNotFoundError(f"No existe: {safe_name}")

    # FastMCP embebe el binario como BlobResourceContents (base64)
    return File(path=str(path), format="xlsx")

FILES_TOOLS: List = [
    get_dataset,
    # get_excel_file
]

from typing import Any, Dict, Optional, List
from utils.json_df import load_dataset_from_json

def get_dataset(data_set_reference: str) -> Dict[str, Any]:
    """Retrieve a dataset previously saved from an user query.

    Args:
        data_set_reference: The filename or identifier of the saved dataset.
    Returns:
        Dict[str, Any]: The dataset loaded from JSON.
    """
    

    dataset = load_dataset_from_json(data_set_reference)
    return dataset

FILES_TOOLS: List = [
    get_dataset
]
import json
from json import JSONDecodeError
from typing import Dict


def get_error(error: Exception) -> Dict[str, str]:
    """
    Extract the error details from an exception.

    Args:
        error (Exception): The exception object.

    Returns:
        Dict[str, str]: Dictionary containing the error details.

    """
    try:
        return json.loads(str(error))
    except JSONDecodeError:
        return {"error": str(error)}

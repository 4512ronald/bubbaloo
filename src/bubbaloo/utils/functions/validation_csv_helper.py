from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubbaloo.services.pipeline import PipelineState
from bubbaloo.utils.interfaces.pipeline_logger import ILogger
from bubbaloo.utils.interfaces.storage_client import IStorageManager


def validate_csv_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Validates the parameters required for CSV file processing in a data pipeline.

    This function checks if the provided parameters match the expected types
    for various components such as Spark session, schema, logger, and storage
    client interfaces. It is essential to ensure that the correct and expected
    types of arguments are passed for the CSV file processing.

    Args:
        params (Dict[str, Any]): A dictionary of parameters to be validated. The expected
            keys and their corresponding types are:
            - 'objects_to_validate': List of file paths to validate (list).
            - 'read_options': Read options for Spark DataFrame (dict).
            - 'spark_schema': Expected Spark DataFrame schema (StructType).
            - 'spark': Spark session instance (SparkSession).
            - 'logger': Logger interface (ILogger).
            - 'storage_client': Interface for storage operations (IStorageManager).
            - 'context': Context object for tracking pipeline errors (PipelineState).
            - 'error_path': File path to store invalid files (str).

    Returns:
        Dict[str, Any]: The validated parameters.

    Raises:
        ValueError: If a required parameter is missing or if a parameter is of an incorrect type.
    """

    param_types = {
        "objects_to_validate": list,
        "read_options": dict,
        "spark_schema": StructType,
        "spark": SparkSession,
        "logger": ILogger,
        "storage_client": IStorageManager,
        "context": PipelineState,
        "error_path": str,
    }
    for param, expected_type in param_types.items():
        if param not in params or not isinstance(params[param], expected_type):
            raise ValueError(f"Se esperaba '{param}' del tipo {expected_type.__name__}")

    return params

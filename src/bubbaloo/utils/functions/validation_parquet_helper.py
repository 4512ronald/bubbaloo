import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud.storage import Blob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.utils.interfaces.storage_client import IStorageManager
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


def validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates the provided parameters against expected types.

    Ensures that each key in the param_types dictionary exists in the params
    argument and that each has the correct type. Raises a ValueError if a
    parameter is missing or has an incorrect type.

    Args:
        params (Dict[str, Any]): The parameters to validate.

    Returns:
        Dict[str, Any]: The validated parameters.

    Raises:
        ValueError: If a parameter is missing or has an incorrect type.
    """
    param_types = {
        "objects_to_validate": list,
        "logger": ILogger,
        "spark": SparkSession,
        "context": PipelineState,
        "spark_schema": StructType,
        "pyarrow_schema": pa.Schema,
        "storage_client": IStorageManager,
        "error_path": str,
    }
    for param, expected_type in param_types.items():
        if param not in params or not isinstance(params[param], expected_type):
            raise ValueError(f"Expected '{param}' of type {expected_type.__name__}")

    return params


def compare_schemas(required_schema: pa.Schema, input_schema: pa.Schema, path: str = '') -> List[str]:
    """
    Compares two PyArrow schemas and identifies differences.

    Recursively checks for differences in field names and types between two schemas.
    Differences are returned as a list of strings indicating the paths to the differing fields.

    Args:
        required_schema (pa.Schema): The schema that is expected.
        input_schema (pa.Schema): The schema to compare against the required schema.
        path (str, optional): The base path for the fields being compared. Defaults to an empty string.

    Returns:
        List[str]: A list of strings indicating the paths to the differing fields.
    """
    fields1 = {field.name: field for field in required_schema}
    fields2 = {field.name: field for field in input_schema}

    differences = [
        f"{path}.{field}" if path else field
        for field in set(fields1).symmetric_difference(set(fields2))
    ]

    for field in set(fields1).intersection(set(fields2)):
        if isinstance(fields1[field].type, pa.StructType):
            differences.extend(
                compare_schemas(
                    fields1[field].type,
                    fields2[field].type,
                    f"{path}.{field}" if path else field
                )
            )
        elif (
                isinstance(fields1[field].type, pa.ListType)
                and isinstance(fields1[field].type.value_type, pa.StructType)
        ):
            differences.extend(
                compare_schemas(
                    fields1[field].type.value_type,
                    fields2[field].type.value_type,
                    f"{path}.{field}" if path else field
                )
            )
        elif fields1[field].type != fields2[field].type:
            differences.append(f"{path}.{field}" if path else field)

    return differences


def get_pyarrow_schema(path: str) -> pa.Schema:
    """
    Retrieves the PyArrow schema for a Parquet file.

    Opens a Parquet file using GCSFileSystem and extracts its schema.

    Args:
        path (str): The path to the Parquet file.

    Returns:
        pa.Schema: The PyArrow schema of the Parquet file.
    """
    parquet_file = pq.ParquetFile(path)

    return parquet_file.schema.to_arrow_schema()


def get_message(error: Exception) -> str:
    """
    Extracts and returns the message from a JSON-formatted exception.

    Args:
        error (Exception): The exception to extract the message from.

    Returns:
        str: The extracted message.
    """
    formatted_error = json.loads(str(error))
    return formatted_error["message"]


def identify_error(error: Exception) -> str:
    """
    Identify the error message and return a corresponding error description.

    Args:
        error (Exception): The error object.

    Returns:
        str: The error description.
    """
    exception_str = str(error).lower()

    error_mapping: Dict[str, str] = {
        "is not a parquet file": "They are not Parquet files",
        "unable to infer schema for parquet. it must be specified manually.": "Error while reading file"
    }

    return next(
        (
            output_message
            for error_message, output_message in error_mapping.items()
            if error_message in exception_str
        ),
        f"Unknown error: {exception_str}"
    )

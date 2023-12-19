import json
from json import JSONDecodeError
from typing import List, Dict, Any

from gcsfs import GCSFileSystem
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.services.pipeline.config import Config
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
        "logger": ILogger,
        "spark": SparkSession,
        "context": PipelineState,
        "conf": Config,
        "schema": StructType,
        "target_schema": pa.Schema,
        "source": str,
        "storage_manager": IStorageManager,
        "time_delta": int,
        "project": str,
        "error_path": str,
        "source_path": str
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
        elif isinstance(fields1[field].type, pa.ListType) and isinstance(fields1[field].type.value_type, pa.StructType):
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


def get_pyarrow_schema(path: str, project: str) -> pa.Schema:
    """
    Retrieves the PyArrow schema for a Parquet file stored in GCS.

    Opens a Parquet file using GCSFileSystem and extracts its schema.

    Args:
        path (str): The path to the Parquet file in GCS.
        project (str): The GCP project name.

    Returns:
        pa.Schema: The PyArrow schema of the Parquet file.
    """
    fs = GCSFileSystem(project=project)
    file = fs.open(path)
    parquet_file = pq.ParquetFile(file)

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
    Identifies and returns a user-friendly error message based on the given exception.

    Maps known error messages to more descriptive, user-friendly ones. If the error
    is not recognized, returns a default message with the original error string.

    Args:
        error (Exception): The exception to identify.

    Returns:
        str: A user-friendly description of the error.
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


def _get_stats(history: DataFrame, statistic: str) -> int:
    """
    Retrieves a specific statistic from the operationMetrics column of a DataFrame.

    Args:
        history (DataFrame): A DataFrame containing operation metrics.
        statistic (str): The name of the statistic to retrieve.

    Returns:
        int: The value of the requested statistic.
    """
    ordered_history = history.orderBy(col("timestamp").desc())

    try:
        stat = ordered_history.select(col("operationMetrics").getItem(statistic)).take(1)[0][0]
    except (IndexError, TypeError):
        stat = 0

    return stat


def get_stats(spark: SparkSession, path: str) -> Dict[str, int]:
    """
     Retrieves various statistics for operations performed on a Delta table.

     Gathers statistics like the number of rows updated, inserted, or deleted for the
     last operation performed on a Delta table, identified by its path.

     Args:
         spark (SparkSession): The SparkSession to execute SQL queries.
         path (str): The path to the Delta table.

     Returns:
         Dict[str, int]: A dictionary containing various statistics.
     """
    count = spark.sql(f"SELECT COUNT(*) FROM delta.`{path}`")

    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")

    last_operation = (
        history
        .filter(~col("operation").isin(["OPTIMIZE", "VACUUM END", "VACUUM START"]))
        .orderBy(col("timestamp").desc())
        .select(col("operation"))
        .first()[0]
    )

    filtered_history = history.where(col("operation") == last_operation)

    if last_operation == "MERGE":
        return {
            "numRowsUpdated": _get_stats(filtered_history, "numTargetRowsUpdated"),
            "numRowsInserted": _get_stats(filtered_history, "numTargetRowsInserted"),
            "numRows": count.take(1)[0][0]
        }

    deletion_history = history.where(col("operation") == "DELETE")

    return {
        "numOutputRows": _get_stats(filtered_history, "numOutputRows"),
        "numDeletedRows": _get_stats(deletion_history, "numDeletedRows"),
        "numRows": count.take(1)[0][0]
    }


def get_error(error: Exception) -> Dict[str, str]:
    """
    Attempts to parse an exception into a JSON-formatted dictionary.

    If the exception cannot be parsed as JSON, returns a dictionary with the error
    message as a string.

    Args:
        error (Exception): The exception to parse.

    Returns:
        Dict[str, str]: A dictionary representation of the error.
    """
    try:
        return json.loads(str(error))
    except JSONDecodeError:
        return {"error": str(error)}

import json
from json import JSONDecodeError
from typing import List, Dict

from dynaconf import Dynaconf
from gcsfs import GCSFileSystem
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def compare_schemas(required_schema: pa.Schema, input_schema: pa.Schema, path: str = '') -> List[str]:
    """
    Compare two PyArrow schemas and return the differences.

    Args:
        required_schema (pa.Schema): The required schema.
        input_schema (pa.Schema): The input schema.
        path (str): The path of the schema (default: '').

    Returns:
        List[str]: List of differences between the schemas.
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


def get_pyarrow_schema(path: str, conf: Dynaconf) -> pa.Schema:
    """
    Get the PyArrow schema of a Parquet file.

    Args:
        path (str): The file path.
        conf (Dynaconf): The configuration instance.

    Returns:
        pa.Schema: The PyArrow schema of the Parquet file.
    """
    fs = GCSFileSystem(project=conf.default_values.project)
    file = fs.open(path)
    parquet_file = pq.ParquetFile(file)

    return parquet_file.schema.to_arrow_schema()


def get_message(error: Exception) -> str:
    """
    Extract the error message from an exception.

    Args:
        error (Exception): The exception object.

    Returns:
        str: The error message.
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


def _get_stats(history: DataFrame, statistic: str) -> int:
    """
    Get the value of a specific statistic from the history DataFrame.

    Args:
        history (DataFrame): The history DataFrame.
        statistic (str): The name of the statistic.

    Returns:
        int: The value of the statistic.
    """
    ordered_history = history.orderBy(col("timestamp").desc())

    try:
        stat = ordered_history.select(col("operationMetrics").getItem(statistic)).take(1)[0][0]
    except (IndexError, TypeError):
        stat = 0

    return stat


def get_stats(spark: SparkSession, path: str) -> Dict[str, int]:
    """
    Get statistics about a Delta table.

    Args:
        spark (SparkSession): The SparkSession instance.
        path (str): The path to the Delta table.

    Returns:
        Dict[str, int]: Dictionary containing the statistics.
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

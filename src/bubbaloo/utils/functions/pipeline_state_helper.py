from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


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


# noinspection SqlNoDataSourceInspection
def get_metrics_from_delta_table(spark: SparkSession, path: str) -> Dict[str, int]:
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

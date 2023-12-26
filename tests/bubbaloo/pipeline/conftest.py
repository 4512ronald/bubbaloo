from typing import Callable

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import functions as f

from bubbaloo.pipeline.stages import Extract, Transform, Load


@pytest.fixture(scope="session")
def batch_extract():
    """
    A pytest fixture for creating an ExtractStage instance.

    This fixture defines and returns an ExtractStage class instance. The ExtractStage class includes a static method
    to define a schema and an execution method to read data in parquet format according to this schema.

    Returns:
        ExtractStage: An instance of the ExtractStage class for batch extraction.
    """
    class ExtractStage(Extract):

        @staticmethod
        def schema():
            return StructType(
                [
                    StructField("ComunicacionID", StringType(), True),
                    StructField("UserID", StringType(), True),
                    StructField("Fecha", StringType(), True),
                    StructField("Estado", StringType(), True),
                ]
            )

        def execute(self) -> DataFrame:
            return (
                self.spark
                .read
                .schema(self.schema())
                .parquet(self.conf.raw_path)
            )

    return ExtractStage()


@pytest.fixture(scope="session")
def batch_transform_1():
    """
    A pytest fixture for creating a TransformStage1 instance.

    This fixture defines and returns a TransformStage1 class instance. The TransformStage1 class contains an
    execution method that adds a new column with a fixed value to a DataFrame.

    Returns:
        TransformStage1: An instance of the TransformStage1 class for batch transformation.
    """
    class TransformStage1(Transform):

        def execute(self, dataframe: DataFrame) -> DataFrame:
            last_df = dataframe.withColumn("descripcion", f.lit("Holis"))

            return last_df
    return TransformStage1()


@pytest.fixture(scope="session")
def batch_transform_2():
    """
    A pytest fixture for creating a TransformStage2 instance.

    This fixture defines and returns a TransformStage2 class instance. The TransformStage2 class contains an
    execution method that adds a timestamp column to a DataFrame.

    Returns:
        TransformStage2: An instance of the TransformStage2 class for batch transformation.
    """
    class TransformStage2(Transform):

        def execute(self, dataframe: DataFrame) -> DataFrame:
            last_df = dataframe.withColumn("fecha_ingreso", f.current_timestamp().cast("timestamp"))

            return last_df
    return TransformStage2()


@pytest.fixture(scope="session")
def batch_load():
    """
    A pytest fixture for creating a LoadStage instance.

    This fixture defines and returns a LoadStage class instance. The LoadStage class includes an execution method
    that writes a DataFrame to a specified location in parquet format, with overwrite capabilities.

    Returns:
        LoadStage: An instance of the LoadStage class for batch loading.
    """
    class LoadStage(Load):

        def execute(
                self,
                dataframe: DataFrame,
                transform: Callable[..., None] | DataFrame | None
        ) -> DataStreamWriter | None:
            return (
                dataframe
                .coalesce(1)
                .write
                .format("parquet")
                .mode("overwrite")
                .save(self.conf.trusted_path)
            )

    return LoadStage()


@pytest.fixture(scope="session")
def streaming_extract():
    """
    A pytest fixture for creating a streaming ExtractStage instance.

    This fixture defines and returns an ExtractStage class instance for streaming data. It includes a static schema
    method and an execution method to read streaming data in parquet format according to this schema.

    Returns:
        ExtractStage: An instance of the ExtractStage class for streaming extraction.
    """
    class ExtractStage(Extract):

        @staticmethod
        def schema():
            return StructType(
                [
                    StructField("ComunicacionID", StringType(), True),
                    StructField("UserID", StringType(), True),
                    StructField("Fecha", StringType(), True),
                    StructField("Estado", StringType(), True),
                ]
            )

        def execute(self) -> DataFrame:
            return (
                self.spark
                .readStream
                .schema(self.schema())
                .parquet(self.conf.raw_path)
            )

    return ExtractStage()


@pytest.fixture(scope="session")
def streaming_transform():
    """
    A pytest fixture for creating a streaming TransformStage instance.

    This fixture defines and returns a TransformStage class instance. The TransformStage class includes an execution
    method that defines a function to transform each batch of streaming data, adding a new column with a fixed value.

    Returns:
        TransformStage: An instance of the TransformStage class for streaming transformation.
    """
    class TransformStage(Transform):
        def execute(self, *args) -> Callable[..., None]:
            def batch_f(batch: DataFrame, batch_id):
                modified = batch.withColumn("descripcion",  f.lit("Holis"))
                modified.coalesce(1).write.format("parquet").mode("append").save(self.conf.trusted_path)

            return batch_f

    return TransformStage()


@pytest.fixture(scope="session")
def streaming_load():
    """
    A pytest fixture for creating a streaming LoadStage instance.

    This fixture defines and returns a LoadStage class instance. The LoadStage class includes an execution method that
    sets up the streaming write operation with specific configurations, such as checkpoint location and trigger
    conditions.

    Returns:
        LoadStage: An instance of the LoadStage class for streaming loading.
    """
    class LoadStage(Load):

        def execute(
                self,
                dataframe: DataFrame,
                transform: Callable[..., None | DataFrame] | None
        ) -> DataStreamWriter | None:

            return (
                dataframe
                .writeStream
                .format("parquet")
                .trigger(once=True)
                .option("checkpointLocation", f"{self.conf.trusted_path}/_checkpoint")
                .foreachBatch(transform)
            )

    return LoadStage()


@pytest.fixture(scope="session")
def error_extract():
    """
    A pytest fixture for creating an error-prone ExtractStage instance.

    This fixture defines and returns an ErrorExtractStage class instance. The ErrorExtractStage class includes an
    execution method that intentionally raises an exception, simulating an error during the extraction phase.

    Returns:
        ErrorExtractStage: An instance of the ErrorExtractStage class that raises an exception during execution.
    """
    class ErrorExtractStage(Extract):
        def execute(self) -> DataFrame:
            raise Exception("Error in extract stage")

    return ErrorExtractStage()


@pytest.fixture(scope="session")
def error_transform():
    """
    A pytest fixture for creating an error-prone TransformStage instance.

    This fixture defines and returns an ErrorTransformStage class instance. The ErrorTransformStage class includes an
    execution method that defines a function, which raises an exception, simulating an error during the transformation
    phase.

    Returns:
        ErrorTransformStage: An instance of the ErrorTransformStage class that raises an exception during
        transformation.
    """
    class ErrorTransformStage(Transform):
        def execute(self, *args) -> Callable[..., None]:
            def batch_f(batch: DataFrame, batch_id): # noqa
                raise Exception("Error in transform stage")

            return batch_f

    return ErrorTransformStage()


@pytest.fixture(scope="session")
def error_load():
    """
    A pytest fixture for creating an error-prone LoadStage instance.

    This fixture defines and returns an ErrorLoadStage class instance. The ErrorLoadStage class includes an execution
    method that intentionally raises an exception, simulating an error during the loading phase.

    Returns:
        ErrorLoadStage: An instance of the ErrorLoadStage class that raises an exception during loading.
    """
    class ErrorLoadStage(Load):
        def execute(
                self,
                dataframe: DataFrame,
                transform: Callable[..., None | DataFrame] | None
        ) -> DataStreamWriter | None:
            raise Exception("Error in load stage")

    return ErrorLoadStage()

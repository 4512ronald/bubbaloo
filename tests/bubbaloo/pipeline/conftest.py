from typing import Callable

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import functions as f

from bubbaloo.pipeline.stages import Extract, Transform, Load


@pytest.fixture(scope="session")
def batch_extract():
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
    class TransformStage1(Transform):

        def execute(self, dataframe: DataFrame) -> DataFrame:
            last_df = dataframe.withColumn("descripcion", f.lit("Holis"))

            return last_df
    return TransformStage1()


@pytest.fixture(scope="session")
def batch_transform_2():
    class TransformStage2(Transform):

        def execute(self, dataframe: DataFrame) -> DataFrame:
            last_df = dataframe.withColumn("fecha_ingreso", f.current_timestamp().cast("timestamp"))

            return last_df
    return TransformStage2()


@pytest.fixture(scope="session")
def batch_load():
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
    class TransformStage(Transform):
        def execute(self, *args) -> Callable[..., None]:
            def batch_f(batch: DataFrame, batch_id):
                modified = batch.withColumn("descripcion",  f.lit("Holis"))
                modified.coalesce(1).write.format("parquet").mode("append").save(self.conf.trusted_path)

            return batch_f

    return TransformStage()


@pytest.fixture(scope="session")
def streaming_load():
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
    class ErrorExtractStage(Extract):
        def execute(self) -> DataFrame:
            raise Exception("Error in extract stage")

    return ErrorExtractStage()


@pytest.fixture(scope="session")
def error_transform():
    class ErrorTransformStage(Transform):
        def execute(self, *args) -> Callable[..., None]:
            def batch_f(batch: DataFrame, batch_id): # noqa
                raise Exception("Error in transform stage")

            return batch_f

    return ErrorTransformStage()


@pytest.fixture(scope="session")
def error_load():
    class ErrorLoadStage(Load):
        def execute(
                self,
                dataframe: DataFrame,
                transform: Callable[..., None | DataFrame] | None
        ) -> DataStreamWriter | None:
            raise Exception("Error in load stage")

    return ErrorLoadStage()

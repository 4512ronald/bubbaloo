import shutil
from typing import Callable

import pytest
import tempfile
import toml
import os

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql import functions as f

from bubbaloo.pipeline.stages import Extract, Transform, Load
from bubbaloo.services.local import Logger
from bubbaloo.services.pipeline import Measure, GetSpark, PipelineState, Config


@pytest.fixture(scope="session", autouse=True)
def spark():
    return GetSpark()


@pytest.fixture(scope="session")
def logger():
    return Logger()


@pytest.fixture(scope="session")
def measure():
    return Measure()


@pytest.fixture(scope="session")
def state():
    return PipelineState()


@pytest.fixture(scope="session", autouse=True)
def raw_temp_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("raw")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session", autouse=True)
def trusted_temp_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("trusted")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session", autouse=True)
def temp_config_file(raw_temp_dir, trusted_temp_dir):
    config_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".toml", mode="w+")

    config_data = {
        "development": {
            "raw_path": str(raw_temp_dir),
            "trusted_path": str(trusted_temp_dir),
            "trigger": {"once": "true"}
        }
    }
    toml.dump(config_data, config_temp_file)
    config_temp_file.close()

    yield config_temp_file.name

    os.remove(config_temp_file.name)


@pytest.fixture(scope="session")
def config(temp_config_file):
    return Config("development", temp_config_file)


@pytest.fixture(scope="session")
def raw_data_parquet(spark, config):
    schema = StructType(
        [
            StructField("ComunicacionID", StringType(), nullable=False),
            StructField("UserID", StringType(), nullable=False),
            StructField("Fecha", StringType(), nullable=False),
            StructField("Estado", StringType(), nullable=False),
        ]
    )

    data = [
        (101, 1, "2023-10-10", "Enviado"),
        (102, 1, "2023-10-11", "Leído"),
        (103, 2, "2023-10-11", "Enviado"),
        (104, 3, "2023-10-11", "Leído"),
        (105, 3, "2023-10-11", "Leído"),
        (106, 4, "2023-10-11", "Enviado"),
        (107, 4, "2023-10-11", "Leído"),
        (108, 4, "2023-10-11", "Leído"),
        (109, 5, "2023-10-11", "Enviado"),
        (110, 5, "2023-10-11", "Leído"),
        (112, 5, "2023-10-11", "Enviado"),
        (113, 6, "2023-10-28", "Leído")
    ]

    df = spark.createDataFrame(data, schema=schema)

    df.write.mode("overwrite").parquet(config.raw_path)

    return df


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

import shutil

import pytest
import tempfile
import toml
import os

from pyspark.sql.types import StructType, StringType, StructField

from bubbaloo.services.local import Logger
from bubbaloo.services.pipeline import Measure, GetSpark, PipelineState, Config


@pytest.fixture(scope="session")
def spark():
    spark = GetSpark()
    spark.sparkContext.setLogLevel("INFO")
    return spark


@pytest.fixture(scope="session")
def logger():
    return Logger()


@pytest.fixture(scope="session")
def measure():
    return Measure()


@pytest.fixture(scope="session")
def state():
    return PipelineState()


@pytest.fixture(scope="session")
def temp_config_file(raw_temp_dir, trusted_temp_dir):
    config_temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".toml", mode="w+")

    config_data = {
        "development": {
            "raw_path": str(raw_temp_dir),
            "trusted_path": str(trusted_temp_dir),
            "trusted_path2": str(trusted_temp_dir) + "_2",
            "trigger": {"once": "true"}
        }
    }
    toml.dump(config_data, config_temp_file)
    config_temp_file.close()

    yield config_temp_file.name

    os.remove(config_temp_file.name)


@pytest.fixture(scope="session")
def env():
    return "development"


@pytest.fixture(scope="session")
def config(temp_config_file, env):
    return Config(env, temp_config_file)


@pytest.fixture(scope="session")
def raw_temp_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("raw")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def trusted_temp_dir(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("trusted")
    yield temp_dir
    shutil.rmtree(temp_dir)


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

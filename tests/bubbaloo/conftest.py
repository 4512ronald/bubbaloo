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
    """
    A pytest fixture that creates and returns a Spark session.

    Returns:
        GetSpark: An instance of the GetSpark class with the Spark context log level set to "INFO".
    """
    spark = GetSpark()
    spark.sparkContext.setLogLevel("INFO")
    return spark


@pytest.fixture(scope="session")
def logger():
    """
    A pytest fixture that creates and returns a Logger instance.

    Returns:
        Logger: An instance of the Logger class.
    """
    return Logger()


@pytest.fixture(scope="session")
def measure():
    """
    A pytest fixture that creates and returns a Measure instance.

    Returns:
        Measure: An instance of the Measure class.
    """
    return Measure()


@pytest.fixture(scope="session")
def state():
    """
    A pytest fixture that creates and returns a PipelineState instance.

    Returns:
        PipelineState: An instance of the PipelineState class.
    """
    return PipelineState()


@pytest.fixture(scope="session")
def temp_config_file(raw_temp_dir, trusted_temp_dir):
    """
    A pytest fixture that creates a temporary configuration file and returns its path.

    Args:
        raw_temp_dir (Path): The temporary directory for raw data.
        trusted_temp_dir (Path): The temporary directory for trusted data.

    Returns:
        str: The path to the created temporary configuration file.
    """
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
    """
    A pytest fixture that provides a mock environment name.

    Returns:
        str: The name of the mock environment.
    """
    return "development"


@pytest.fixture(scope="session")
def config(temp_config_file, env):
    """
    A pytest fixture that creates and returns a Config instance using a temporary configuration file and a mock
    environment.

    Args:
        temp_config_file (str): The path to the temporary configuration file.
        env (str): The mock environment name.

    Returns:
        Config: An instance of the Config class.
    """
    return Config(env, temp_config_file)


@pytest.fixture(scope="session")
def raw_temp_dir(tmp_path_factory):
    """
    A pytest fixture that creates and returns a temporary directory for storing raw data.

    Returns:
        Path: The temporary directory path for raw data.
    """
    temp_dir = tmp_path_factory.mktemp("raw")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def trusted_temp_dir(tmp_path_factory):
    """
    A pytest fixture that creates and returns a temporary directory for storing trusted data.

    Returns:
        Path: The temporary directory path for trusted data.
    """
    temp_dir = tmp_path_factory.mktemp("trusted")
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="session")
def raw_data_parquet(spark, config):
    """
    A pytest fixture that creates a DataFrame with test data and writes it to a parquet file in the raw data path.

    Args:
        spark (GetSpark): The Spark session fixture.
        config (Config): The configuration fixture.

    Returns:
        DataFrame: A Spark DataFrame containing the test data.
    """
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

    df = spark.createDataFrame(data, schema=schema) # noqa

    df.write.mode("overwrite").parquet(config.raw_path)

    return df

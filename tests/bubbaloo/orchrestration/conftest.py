import textwrap
from pathlib import Path

import pytest


@pytest.fixture
def flows_to_execute():
    """
    A pytest fixture that provides a list of entities to execute.

    Returns:
        list: A list of strings representing the names of entities.
              Currently includes "entity1" and "entity2".
    """

    return ["entity1", "entity2"]


@pytest.fixture
def flows_to_execute_with_errors():
    """
    A pytest fixture that provides a list of entities, including one that may cause errors.

    Returns:
        list: A list of strings representing the names of entities.
              Includes "entity1", "entity2", and "entity3", where "entity3" may cause errors.
    """
    return ["entity1", "entity2", "entity3"]


@pytest.fixture
def flows_dir(tmp_path):
    """
    A pytest fixture that creates and returns a directory for storing flow entities.

    Args:
        tmp_path (Path): A Path object provided by pytest for creating temporary directories.

    Returns:
        Path: A Path object representing the directory where flow entities are stored.
    """
    flows_dir = tmp_path / "flows"
    flows_dir.mkdir()
    return flows_dir


@pytest.fixture
def entity1_dir(flows_dir):
    """
    A pytest fixture that creates and returns a directory for 'entity1'.

    Args:
        flows_dir (Path): The base directory for flow entities.

    Returns:
        Path: A Path object representing the directory specific to 'entity1'.
    """
    entity1_dir = Path(flows_dir) / "entity1"
    entity1_dir.mkdir()
    return entity1_dir


@pytest.fixture
def entity2_dir(flows_dir):
    """
    A pytest fixture that creates and returns a directory for 'entity2'.

    Args:
        flows_dir (Path): The base directory for flow entities.

    Returns:
        Path: A Path object representing the directory specific to 'entity2'.
    """
    entity2_dir = Path(flows_dir) / "entity2"
    entity2_dir.mkdir()
    return entity2_dir


@pytest.fixture
def entity3_dir(flows_dir):
    """
    A pytest fixture that creates and returns a directory for 'entity3'.

    Args:
        flows_dir (Path): The base directory for flow entities.

    Returns:
        Path: A Path object representing the directory specific to 'entity3'.
    """
    entity3_dir = Path(flows_dir) / "entity3"
    entity3_dir.mkdir()
    return entity3_dir


@pytest.fixture
def entity4_dir(flows_dir):
    """
    A pytest fixture that creates and returns a directory for 'entity4'.

    Args:
        flows_dir (Path): The base directory for flow entities.

    Returns:
        Path: A Path object representing the directory specific to 'entity4'.
    """
    entity4_dir = Path(flows_dir) / "entity4"
    entity4_dir.mkdir()
    return entity4_dir


@pytest.fixture
def write_modules_entity_1(extract_module_entity_1, transform_module_entity_1, load_module_entity_1):
    """
    A pytest fixture for writing modules related to 'entity1'.

    This fixture utilizes the extract, transform, and load modules specific to 'entity1'.

    Args:
        extract_module_entity_1 (str): Path to the extract module for 'entity1'.
        transform_module_entity_1 (str): Path to the transform module for 'entity1'.
        load_module_entity_1 (str): Path to the load module for 'entity1'.
    """
    pass


@pytest.fixture
def write_modules_entity_2(extract_module_entity_2, transform_module_entity_2, load_module_entity_2):
    """
    A pytest fixture for writing modules related to 'entity2'.

    This fixture utilizes the extract, transform, and load modules specific to 'entity2'.

    Args:
        extract_module_entity_2 (str): Path to the extract module for 'entity2'.
        transform_module_entity_2 (str): Path to the transform module for 'entity2'.
        load_module_entity_2 (str): Path to the load module for 'entity2'.
    """
    pass


@pytest.fixture
def write_modules_entity_3(extract_module_entity_3, transform_module_entity_3, load_module_entity_3):
    """
    A pytest fixture for writing modules related to 'entity3'.

    This fixture utilizes the extract, transform, and load modules specific to 'entity3'.

    Args:
        extract_module_entity_3 (str): Path to the extract module for 'entity3'.
        transform_module_entity_3 (str): Path to the transform module for 'entity3'.
        load_module_entity_3 (str): Path to the load module for 'entity3'.
    """
    pass


@pytest.fixture
def write_modules_entity_4(entity4_dir):
    """
    A pytest fixture for writing modules related to 'entity4'.

    Args:
        entity4_dir (Path): The directory path for storing 'entity4' related modules.
    """
    pass


@pytest.fixture
def extract_module_entity_1(entity1_dir):
    """
    A pytest fixture for creating an extraction module for 'entity1'.

    This fixture writes the necessary extraction code to a Python file in 'entity1's directory.

    Args:
        entity1_dir (Path): The directory path for 'entity1'.

    Returns:
        str: The path to the created extraction module for 'entity1'.
    """
    extract_module_path = entity1_dir / "extract.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from pyspark.sql.types import StructType, StringType, StructField
        from bubbaloo.pipeline.stages import Extract

        class ExtractStage(Extract):

            @staticmethod
            def schema():
                return StructType([
                    StructField("ComunicacionID", StringType(), True),
                    StructField("UserID", StringType(), True),
                    StructField("Fecha", StringType(), True),
                    StructField("Estado", StringType(), True),
                ])

            def execute(self) -> DataFrame:
                return self.spark.read.schema(self.schema()).parquet(self.conf.raw_path)
        """)
    extract_module_path.write_text(code)
    return str(extract_module_path)


@pytest.fixture
def transform_module_entity_1(entity1_dir):
    """
    A pytest fixture that creates a transformation module for 'entity1'.

    This fixture generates Python code for a transformation stage, writes it to 'transform.py' in the 'entity1'
    directory, and returns the path to this file. The transformation stage is designed to modify a DataFrame by adding
    a new column with a fixed value.

    Args:
        entity1_dir (Path): The directory path where the 'entity1' modules are stored.

    Returns:
        str: The path to the created transformation module for 'entity1'.
    """
    transform_module_path = entity1_dir / "transform.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from pyspark.sql import functions as f
        from bubbaloo.pipeline.stages import Transform

        class TransformStage1(Transform):

            def execute(self, dataframe: DataFrame) -> DataFrame:
                return dataframe.withColumn("descripcion", f.lit("Holis"))
        """)
    transform_module_path.write_text(code)
    return str(transform_module_path)


@pytest.fixture
def load_module_entity_1(entity1_dir):
    """
    A pytest fixture that creates a loading module for 'entity1'.

    This fixture generates Python code for a load stage, writes it to 'load.py' in the 'entity1' directory, and returns
    the path to this file. The load stage is responsible for writing the DataFrame to a specified location in a parquet
    format, with the capability to overwrite existing data.

    Args:
        entity1_dir (Path): The directory path where the 'entity1' modules are stored.

    Returns:
        str: The path to the created loading module for 'entity1'.
    """
    load_module_path = entity1_dir / "load.py"
    code = textwrap.dedent("""
        from typing import Callable
        from pyspark.sql import DataFrame
        from pyspark.sql.streaming import DataStreamWriter
        from bubbaloo.pipeline.stages import Load

        class LoadStage(Load):

            def execute(self, dataframe: DataFrame, 
                        transform: Callable[..., None] | DataFrame | None) -> DataStreamWriter | None:
                return dataframe.coalesce(1).write.format("parquet").mode("overwrite").save(self.conf.trusted_path)
        """)
    load_module_path.write_text(code)
    return str(load_module_path)


@pytest.fixture
def extract_module_entity_2(entity2_dir):
    """
    A pytest fixture for creating an extraction module for 'entity2'.

    This fixture writes the necessary code for an extraction stage to 'extract.py' in the 'entity2' directory. The
    extraction stage includes a defined schema and functionality to read data in parquet format according to this
    schema.

    Args:
        entity2_dir (Path): The directory path for 'entity2'.

    Returns:
        str: The path to the created extraction module for 'entity2'.
    """
    extract_module_path = entity2_dir / "extract.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from pyspark.sql.types import StructType, StringType, StructField
        from bubbaloo.pipeline.stages import Extract

        class ExtractStage(Extract):

            @staticmethod
            def schema():
                return StructType([
                    StructField("ComunicacionID", StringType(), True),
                    StructField("UserID", StringType(), True),
                    StructField("Fecha", StringType(), True),
                    StructField("Estado", StringType(), True),
                ])

            def execute(self) -> DataFrame:
                return self.spark.read.schema(self.schema()).parquet(self.conf.raw_path)
        """)
    extract_module_path.write_text(code)
    return str(extract_module_path)


@pytest.fixture
def transform_module_entity_2(entity2_dir):
    """
    A pytest fixture for creating transformation modules for 'entity2'.

    This fixture generates Python code for two transformation stages, writing it to 'transform.py' in the 'entity2'
    directory. Each transformation stage modifies the DataFrame, either by adding a fixed value column or a timestamp
    column.

    Args:
        entity2_dir (Path): The directory path for 'entity2'.

    Returns:
        str: The path to the created transformation modules for 'entity2'.
    """
    transform_module_path = entity2_dir / "transform.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from pyspark.sql import functions as f
        from bubbaloo.pipeline.stages import Transform

        class TransformStage1(Transform):

            def execute(self, dataframe: DataFrame) -> DataFrame:
                return dataframe.withColumn("descripcion", f.lit("Holis"))

        class TransformStage2(Transform):

            def execute(self, dataframe: DataFrame) -> DataFrame:
                return dataframe.withColumn("fecha_ingreso", f.current_timestamp().cast("timestamp"))
        """)
    transform_module_path.write_text(code)
    return str(transform_module_path)


@pytest.fixture
def load_module_entity_2(entity2_dir):
    """
    A pytest fixture for creating a loading module for 'entity2'.

    This fixture writes code for a load stage to 'load.py' in the 'entity2' directory. The load stage is designed to
    write the DataFrame to a specified location in parquet format, with overwrite capabilities.

    Args:
        entity2_dir (Path): The directory path for 'entity2'.

    Returns:
        str: The path to the created loading module for 'entity2'.
    """
    load_module_path = entity2_dir / "load.py"
    code = textwrap.dedent("""
        from typing import Callable
        from pyspark.sql import DataFrame
        from pyspark.sql.streaming import DataStreamWriter
        from bubbaloo.pipeline.stages import Load

        class LoadStage(Load):

            def execute(self, dataframe: DataFrame, 
                        transform: Callable[..., None] | DataFrame | None) -> DataStreamWriter | None:
                return dataframe.coalesce(1).write.format("parquet").mode("overwrite").save(self.conf.trusted_path2)
        """)
    load_module_path.write_text(code)
    return str(load_module_path)


@pytest.fixture
def extract_module_entity_3(entity3_dir):
    """
    A pytest fixture for creating an extraction module for 'entity3' that intentionally raises an exception.

    This fixture writes code for an extraction stage to 'extract.py' in the 'entity3' directory. The extraction stage
    is designed to raise an exception when executed.

    Args:
        entity3_dir (Path): The directory path for 'entity3'.

    Returns:
        str: The path to the created extraction module for 'entity3'.
    """
    extract_module_path = entity3_dir / "extract.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from bubbaloo.pipeline.stages import Extract

        class ExtractStage(Extract):

            def execute(self) -> DataFrame:
                raise Exception("Error")
        """)
    extract_module_path.write_text(code)
    return str(extract_module_path)


@pytest.fixture
def transform_module_entity_3(entity3_dir):
    """
    A pytest fixture for creating a transformation module for 'entity3' that intentionally raises an exception.

    This fixture writes code for a transformation stage to 'transform.py' in the 'entity3' directory. The
    transformation stage is designed to raise an exception when executed.

    Args:
        entity3_dir (Path): The directory path for 'entity3'.

    Returns:
        str: The path to the created transformation module for 'entity3'.
    """
    transform_module_path = entity3_dir / "transform.py"
    code = textwrap.dedent("""
        from pyspark.sql import DataFrame
        from bubbaloo.pipeline.stages import Transform

        class TransformStage1(Transform):

            def execute(self, dataframe: DataFrame) -> DataFrame:
                raise Exception("Error")
        """)
    transform_module_path.write_text(code)
    return str(transform_module_path)


@pytest.fixture
def load_module_entity_3(entity3_dir):
    """
    A pytest fixture for creating a loading module for 'entity3' that intentionally raises an exception.

    This fixture writes code for a load stage to 'load.py' in the 'entity3' directory. The load stage is designed to
    raise an exception when executed.

    Args:
        entity3_dir (Path): The directory path for 'entity3'.

    Returns:
        str: The path to the created loading module for 'entity3'.
    """
    load_module_path = entity3_dir / "load.py"
    code = textwrap.dedent("""
        from typing import Callable
        from pyspark.sql import DataFrame
        from pyspark.sql.streaming import DataStreamWriter
        from bubbaloo.pipeline.stages import Load

        class LoadStage(Load):

            def execute(self, dataframe: DataFrame, 
                        transform: Callable[..., None] | DataFrame | None) -> DataStreamWriter | None:
                raise Exception("Error")
        """)
    load_module_path.write_text(code)
    return str(load_module_path)

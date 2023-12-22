import textwrap
from pathlib import Path

import pytest


@pytest.fixture
def flows_to_execute():
    return ["entity1", "entity2"]


@pytest.fixture
def flows_to_execute_with_errors():
    return ["entity1", "entity2", "entity3"]


@pytest.fixture
def flows_dir(tmp_path):
    flows_dir = tmp_path / "flows"
    flows_dir.mkdir()
    return flows_dir


@pytest.fixture
def entity1_dir(flows_dir):
    entity1_dir = Path(flows_dir) / "entity1"
    entity1_dir.mkdir()
    return entity1_dir


@pytest.fixture
def entity2_dir(flows_dir):
    entity2_dir = Path(flows_dir) / "entity2"
    entity2_dir.mkdir()
    return entity2_dir


@pytest.fixture
def entity3_dir(flows_dir):
    entity3_dir = Path(flows_dir) / "entity3"
    entity3_dir.mkdir()
    return entity3_dir


@pytest.fixture
def entity4_dir(flows_dir):
    entity4_dir = Path(flows_dir) / "entity4"
    entity4_dir.mkdir()
    return entity4_dir


@pytest.fixture
def write_modules_entity_1(extract_module_entity_1, transform_module_entity_1, load_module_entity_1):
    pass


@pytest.fixture
def write_modules_entity_2(extract_module_entity_2, transform_module_entity_2, load_module_entity_2):
    pass


@pytest.fixture
def write_modules_entity_3(extract_module_entity_3, transform_module_entity_3, load_module_entity_3):
    pass


@pytest.fixture
def write_modules_entity_4(entity4_dir):
    pass


@pytest.fixture
def extract_module_entity_1(entity1_dir):
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

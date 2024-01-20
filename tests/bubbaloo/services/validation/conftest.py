import glob
import textwrap
from unittest.mock import create_autospec, Mock, MagicMock

import pytest
from google.cloud.storage import Blob
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as f

from bubbaloo.services.local import Logger, LocalFileManager
from bubbaloo.services.pipeline import PipelineState
from bubbaloo.services.validation.parquet import Parquet
from bubbaloo.utils.functions import get_files_days_ago
from bubbaloo.utils.interfaces.storage_client import IStorageManager


@pytest.fixture
def tmp_test_dir(tmp_path):

    test_dir = tmp_path / "base"
    test_dir.mkdir()
    return test_dir


@pytest.fixture
def tmp_error_path(tmp_path):

    error_dir = tmp_path / "error"
    error_dir.mkdir()
    return error_dir


@pytest.fixture
def base_spark_schema():
    return StructType(
        [
            StructField("ComunicacionID", StringType(), nullable=False),
            StructField("UserID", StringType(), nullable=False),
            StructField("Fecha", StringType(), nullable=False),
            StructField("Estado", StringType(), nullable=False),
        ]
    )


@pytest.fixture
def base_pyarrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            ("ComunicacionID", pa.string()),
            ("UserID", pa.string()),
            ("Fecha", pa.string()),
            ("Estado", pa.string()),
        ]
    )


@pytest.fixture
def parquet_data(spark, tmp_test_dir, base_spark_schema):

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

    base_df = spark.createDataFrame(data, schema=base_spark_schema) # noqa

    base_df.coalesce(1).write.mode("append").parquet(str(tmp_test_dir))

    first_transform = (
        base_df
        .drop("UserID")
        .withColumn("FechaActualizacion", f.current_date())
        .withColumn("Descripcion", f.lit("Prueba"))
    )

    first_transform.coalesce(1).write.mode("append").parquet(str(tmp_test_dir))

    no_parquet_file = tmp_test_dir / "no_parquet_file.txt"

    content = textwrap.dedent("""
        Este no es un archivo parquet.
    """)

    no_parquet_file.write_text(content)

    corrupted_parquet_file = tmp_test_dir / "corrupted.parquet"

    corrupted_parquet_file.write_bytes(b"PAR1")


@pytest.fixture
def mock_cloud_storage_manager(tmp_test_dir, parquet_data):
    mock_manager = create_autospec(IStorageManager, instance=True)

    archivos = glob.glob(str(tmp_test_dir / "*"))

    simulated_blobs = [Blob(name=str(archivo), bucket=None) for archivo in archivos]

    mock_manager.list.return_value = simulated_blobs

    mock_manager.move.return_value = None

    mock_manager.copy.return_value = None

    return mock_manager


@pytest.fixture
def mock_pipeline_state():
    mock_state = MagicMock(spec=PipelineState)
    # Configurar el atributo 'errors' para soportar asignaciones
    mock_state.errors = {}
    return mock_state


def test_parquet_validation(mock_pipeline_state, mock_cloud_storage_manager,
                            tmp_test_dir, base_spark_schema, base_pyarrow_schema, spark, tmp_error_path):
    mock_logger = Mock(spec=Logger)
    files = glob.glob(str(tmp_test_dir / "*"))
    client = LocalFileManager()

    filtered_files = client.filter(files, lambda file: get_files_days_ago(file, 5))

    parquet_validator = Parquet(
        objects_to_validate=filtered_files,
        spark_schema=base_spark_schema,
        pyarrow_schema=base_pyarrow_schema,
        spark=spark,
        logger=mock_logger,
        storage_client=client,
        context=mock_pipeline_state,
        error_path=str(tmp_error_path)
    )

    parquet_validator.execute()

    print(parquet_validator.validation_resume)

    assert len(parquet_validator.invalid_blobs) == 3

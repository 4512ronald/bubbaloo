import os
import tempfile
from typing import Dict, List, Generator, Any
import ast
import itertools
import json

import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubbaloo.services.local.logger import Logger
from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.errors.errors import DataFrameSchemaError, CorruptedPathError
from bubbaloo.utils.functions.validation_parquet_helper import (
    compare_schemas,
    get_pyarrow_schema,
    get_message,
    identify_error,
    validate_params
)
from bubbaloo.utils.interfaces.storage_client import IStorageManager
from bubbaloo.utils.interfaces.services_validation import IValidation


class Parquet(IValidation):

    def __init__(self, **kwargs) -> None:
        self._params: Dict[str, Any] = validate_params(kwargs)
        self._objects_to_validate: List[str] = self._params.get("objects_to_validate")
        self._spark_schema: StructType = self._params.get("spark_schema")
        self._pa_schema: pa.Schema = self._params.get("pyarrow_schema")
        self._spark: SparkSession = self._params.get("spark")
        self._logger: Logger = self._params.get("logger")
        self._storage_client: IStorageManager = self._params.get("storage_client")
        self._error_context: PipelineState = self._params.get("context")
        self._error_path: str = self._params.get("error_path")

    def _get_invalid_blobs(self, file_paths: List[str]) -> Generator[Dict[str, str], None, None]:
        for file_path in file_paths:
            error = self._validate_file(file_path)
            if error is not None:
                yield error

    @staticmethod
    def _verify_schema(required_schema: pa.Schema, input_schema: pa.Schema) -> None:

        if differences := compare_schemas(required_schema, input_schema):
            raise DataFrameSchemaError(str(differences))

    def _verify_parquet_file(self, file_path: str) -> None:

        if file_path.endswith("/_SUCCESS") or file_path.endswith("/"):
            return
        if not file_path.endswith(".parquet"):
            raise CorruptedPathError("They are not Parquet files")

        try:
            temp_df = self._spark.read.schema(self._spark_schema).parquet(file_path)
            temp_df.take(1)
        except Exception as error:
            error_message = identify_error(error)
            raise CorruptedPathError(error_message) from error

    def _move_invalid_files(self, invalid_blobs: List[Dict[str, str]]) -> None:
        self._logger.error(
            "Some corrupted files found: \n"
            f"{json.dumps(self._error_context.errors['dataProcessing'])} \n"
            "Corrupted files will not be processed, check these files in storage."
        )

        invalid_path_blobs = [element["path"] for element in invalid_blobs]

        self._storage_client.move(invalid_path_blobs, self._error_path)

        self._logger.info(
            f"Corrupted files have been moved to the following location in storage: \
            '{self._error_path}'."
        )

    def _handle_invalid_files(self, not_valid_blob: List[Dict[str, str]]) -> None:

        sorted_list = sorted(not_valid_blob, key=lambda item: item["error"])
        validation_errors = [
            {"message": key, "files": [item["path"] for item in items]}
            for key, items in itertools.groupby(sorted_list, key=lambda item: item["error"])
        ]

        self._error_context.errors["dataProcessing"] = str(validation_errors)

    def _get_schema_from_temp_copy(self, file_path: str) -> pa.Schema:
        with tempfile.TemporaryDirectory(prefix=os.path.basename(__file__)) as temp_dir:
            temp_file_path = os.path.join(temp_dir, os.path.basename(file_path))
            self._storage_client.copy([file_path], temp_dir)

            return get_pyarrow_schema(temp_file_path)

    def _validate_file(self, path: str) -> Dict[str, str] | None:

        try:
            self._verify_parquet_file(path)
            pa_schema = self._get_schema_from_temp_copy(path)
            self._verify_schema(pa_schema, self._pa_schema)
        except (CorruptedPathError, DataFrameSchemaError) as error:
            message = get_message(error)
            if isinstance(error, CorruptedPathError):
                return {"error": message, "path": path}

            invalid_cols = ast.literal_eval(message)

            return {
                "error": f"schema does not match in the columns: {', '.join(invalid_cols)}",
                "path": path
            }
        except Exception as error:
            return {"error": identify_error(error), "path": path}

    def execute(self) -> None:

        self._logger.info("Checking integrity of the data...")

        if invalid_blobs := list(self._get_invalid_blobs(self._objects_to_validate)):
            self._handle_invalid_files(invalid_blobs)
            self._move_invalid_files(invalid_blobs)
        else:
            self._logger.info("Files validated successfully")

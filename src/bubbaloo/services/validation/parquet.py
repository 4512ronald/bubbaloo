from typing import Dict, List, Generator, Any
from datetime import datetime, timezone, timedelta
import ast
import itertools
import json

import pyarrow as pa
from google.cloud.storage.blob import Blob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubbaloo.services.pipeline.config import Config
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

    # TODO Generalizar el uso de parquet para que no dependa de GCP

    def __init__(self, **kwargs) -> None:
        self._params: Dict[str, Any] = validate_params(kwargs)
        self.schema: StructType = self._params.get("schema")
        self.pa_schema: pa.Schema = self._params.get("target_schema")
        self.source: str = self._params.get("source")
        self.conf: Config = self._params.get("conf")
        self.spark: SparkSession = self._params.get("spark")
        self.logger: Logger = self._params.get("logger")
        self.storage_client: IStorageManager = self._params.get("storage_manager")
        self.error_context: PipelineState = self._params.get("context")
        self.time_delta: int = self._params.get("time_delta")
        self.project: str = self._params.get("project")
        self.error_path: str = self._params.get("error_path")
        self.source_path: str = self._params.get("source_path")

    def _get_file_paths(self, blobs: List[Blob]) -> Generator[str, None, None]:

        # TODO Quitar dependencia de GCP Blob

        days_ago = datetime.now(timezone.utc).date() - timedelta(days=self.time_delta)

        for blob in blobs:
            if blob.updated.date() >= days_ago:
                yield f"gs://{blob.bucket.name}/{blob.name}"

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
            temp_df = self.spark.read.schema(self.schema).parquet(file_path)
            temp_df.take(1)
        except Exception as error:
            error_message = identify_error(error)
            raise CorruptedPathError(error_message) from error

    def _move_invalid_files(self, invalid_blobs: List[Dict[str, str]]) -> None:
        self.logger.error(
            "Some corrupted files found: \n"
            f"{json.dumps(self.error_context.errors['dataProcessing'])} \n"
            "Corrupted files will not be processed, check these files in storage."
        )

        invalid_path_blobs = [element["path"] for element in invalid_blobs]

        self.storage_client.move(invalid_path_blobs, self.error_path)

        self.logger.info(
            f"Corrupted files have been moved to the following location in storage: \
            '{self.error_path}'."
        )

    def _handle_invalid_files(self, not_valid_blob: List[Dict[str, str]]) -> None:

        sorted_list = sorted(not_valid_blob, key=lambda item: item["error"])
        validation_errors = [
            {"message": key, "files": [item["path"] for item in items]}
            for key, items in itertools.groupby(sorted_list, key=lambda item: item["error"])
        ]

        self.error_context.errors["dataProcessing"] = str(validation_errors)

    def _validate_file(self, path: str) -> Dict[str, str] | None:

        # TODO Quitar dependencia de GCP _get_pyarrow_schema

        try:
            self._verify_parquet_file(path)
            pa_schema = get_pyarrow_schema(path, self.project)
            self._verify_schema(pa_schema, self.pa_schema)
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

        self.logger.info("Checking integrity of the data...")

        # TODO Quitar dependencia de GCP list blobs

        blobs = [
            blob
            for blob in self.storage_client.list(self.conf.source.main_source)
            if not (blob.name.endswith("/_SUCCESS") or blob.name.endswith("/"))
        ]

        file_paths = list(self._get_file_paths(blobs))

        if invalid_blobs := list(self._get_invalid_blobs(file_paths)):
            self._handle_invalid_files(invalid_blobs)
            self._move_invalid_files(invalid_blobs)
        else:
            self.logger.info("Files validated successfully")

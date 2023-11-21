from importlib import import_module
from typing import Dict, List, Generator
from datetime import datetime, timezone, timedelta
import ast
import itertools
import json

import pyarrow as pa
from dynaconf import Dynaconf
from google.cloud.storage.blob import Blob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubaloo.services.pipeline.config import Config
from bubaloo.services.cloud.gcp import GCSClient
from bubaloo.services.pipeline.logger import Logger
from bubaloo.services.pipeline.session import Session
from bubaloo.services.pipeline.state import PipelineState
from bubaloo.services.pipeline.errors import DataFrameSchemaError, CorruptedPathError
from bubaloo.utils.functions.validation_parquet_helper import compare_schemas, get_pyarrow_schema, get_message, identify_error


class DataValidator:
    """
    Validates data integrity by checking the schema and file format of Parquet files in Google Cloud Storage.

    Args:
        source (str): The source name.
        schema (pyspark.sql.types.StructType): The required schema for validation.

    Attributes:
        conf (Dynaconf): The configuration object.
        spark (pyspark.sql.SparkSession): The Spark session.
        logger (Logger): The logger.
        gcs_client (GCSClient): The Google Cloud Storage client.
        error_context (PipelineState): The state object for storing error information.
        schema (pyspark.sql.types.StructType): The required schema for validation.
        source (str): The source name.
        pa_schema (pyarrow.Schema): The PyArrow schema for the source.

    """

    validated = False

    def __init__(self, source: str, schema: StructType) -> None:
        self.schema: StructType = schema
        self.source: str = source
        self.conf: Dynaconf = Config.get()
        self.spark: SparkSession = Session.get_or_create()
        self.logger: Logger = Logger(self.__class__.__name__)
        self.gcs_client: GCSClient = GCSClient(self.conf.default_values.project)
        self.error_context: PipelineState = PipelineState()
        self.pa_schema: pa.Schema = import_module(f"shared.schemas.pyarrow.{self.source}").SCHEMA

    def _get_file_paths(self, blobs: List[Blob]) -> Generator[str, None, None]:
        """
        Get the file paths of the Parquet files that were updated after the specified date.

        Args:
            blobs (List[Blob]): List of GCS blobs.

        Yields:
            str: File path.

        """

        days_ago = datetime.now(timezone.utc).date() - timedelta(days=self.conf.default_values.timedelta)

        for blob in blobs:
            if blob.updated.date() >= days_ago:
                yield f"gs://{blob.bucket.name}/{blob.name}"

    def _get_invalid_blobs(self, file_paths: List[str]) -> Generator[Dict[str, str], None, None]:
        """
        Validate the Parquet files and return a list of invalid file paths and corresponding errors.

        Args:
            file_paths (List[str]): List of file paths.

        Yields:
            Dict[str, str]: Invalid file path and error.

        """
        for file_path in file_paths:
            error = self._validate_file(file_path)
            if error is not None:
                yield error

    @staticmethod
    def _verify_schema(required_schema: pa.Schema, input_schema: pa.Schema) -> None:
        """
        Verify if the input schema matches the required schema.

        Args:
            required_schema (pyarrow.Schema): The required schema.
            input_schema (pyarrow.Schema): The input schema.

        Raises:
            DataFrameSchemaError: If the schemas do not match.

        """
        if differences := compare_schemas(required_schema, input_schema):
            raise DataFrameSchemaError(str(differences))

    def _verify_parquet_file(self, file_path: str) -> None:
        """
        Verify the file format and schema of a Parquet file.

        Args:
            file_path (str): The file path.

        Raises:
            CorruptedPathError: If the file path is not a Parquet file or has an invalid schema.

        """
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

        self.gcs_client.move_blobs(invalid_path_blobs, self.conf.source.error_path)

        self.logger.info(
            f"Corrupted files have been moved to the following location in storage: \
            '{self.conf.source.error_path}'."
        )

    def _handle_invalid_files(self, not_valid_blob: List[Dict[str, str]]) -> None:
        """
        Handle the invalid files by grouping them based on errors.

        Args:
            not_valid_blob (List[Dict[str, str]]): List of invalid file paths and errors.

        """
        sorted_list = sorted(not_valid_blob, key=lambda item: item["error"])
        validation_errors = [
            {"message": key, "files": [item["path"] for item in items]}
            for key, items in itertools.groupby(sorted_list, key=lambda item: item["error"])
        ]

        self.error_context.errors["dataProcessing"] = validation_errors

    def _validate_file(self, path: str) -> Dict[str, str] | None:
        """
        Validate a Parquet file by verifying its file format and schema.

        Args:
            path (str): The file path.

        Returns:
            Dict[str, str] | None: The error details if the file is invalid, None otherwise.

        """
        try:
            self._verify_parquet_file(path)
            pa_schema = get_pyarrow_schema(path, self.conf)
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

    def _validate_entity(self) -> None:
        """
        Validate the integrity of the data by checking the file format and schema of Parquet files.

        """
        self.logger.info("Checking integrity of the data...")

        blobs = [
            blob
            for blob in self.gcs_client.get_blobs(self.conf.source.main_source)
            if not (blob.name.endswith("/_SUCCESS") or blob.name.endswith("/"))
        ]

        file_paths = list(self._get_file_paths(blobs))

        if invalid_blobs := list(self._get_invalid_blobs(file_paths)):
            self._handle_invalid_files(invalid_blobs)
            self._move_invalid_files(invalid_blobs)
        else:
            self.logger.info("Files validated successfully")

    @classmethod
    def validate_entity(cls, source, schema) -> None:
        """
        Validates the integrity of the data entity if not already validated.
        """
        instance = cls(source, schema)

        if not cls.validated:
            cls.validated = True
            return instance._validate_entity()

        instance.logger.info("Files already validated")

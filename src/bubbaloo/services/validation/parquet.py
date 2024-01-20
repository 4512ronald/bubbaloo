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
    """
    A class that implements the IValidation interface for validating Parquet files.

    This class includes methods for validating the schema and integrity of Parquet files, handling invalid files,
    and managing errors during validation.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initializes the Parquet validator with various parameters.

        Args: kwargs (dict): A dictionary of parameters including objects to validate, schemas, Spark session,
        logger, storage client, error context, and error path.
        """
        self._params: Dict[str, Any] = validate_params(kwargs)
        self._objects_to_validate: List[str] = self._params.get("objects_to_validate")
        self._spark_schema: StructType = self._params.get("spark_schema")
        self._pa_schema: pa.Schema = self._params.get("pyarrow_schema")
        self._spark: SparkSession = self._params.get("spark")
        self._logger: Logger = self._params.get("logger")
        self._storage_client: IStorageManager = self._params.get("storage_client")
        self._error_context: PipelineState = self._params.get("context")
        self._error_path: str = self._params.get("error_path")
        self._invalid_blobs: List[str] | None = None
        self._validation_resume: List[Dict[str, str]] | str = []

    @property
    def invalid_blobs(self) -> List[str] | None:
        """
        Returns a list of invalid blobs.

        Returns:
            List[str] | None: A list of invalid blobs.
        """
        return self._invalid_blobs

    @property
    def validation_resume(self) -> List[Dict[str, str]] | str:
        """
        Returns a summary of the validation results.

        Returns:
            List[Dict[str, str]] | str: A summary of the validation results.
        """
        return self._validation_resume

    def _get_invalid_blobs(self, file_paths: List[str]) -> Generator[Dict[str, str], None, None]:
        """
        Identifies invalid blobs in the provided file paths.

        Args:
            file_paths (List[str]): A list of file paths to validate.

        Yields:
            Generator[Dict[str, str], None, None]: A generator yielding dictionaries with error details for each invalid
            blob.
        """
        for file_path in file_paths:
            error = self._validate_file(file_path)
            if error is not None:
                yield error

    @staticmethod
    def _verify_schema(required_schema: pa.Schema, input_schema: pa.Schema) -> None:
        """
        Verifies if the input schema matches the required schema.

        Args:
            required_schema (pa.Schema): The required schema to validate against.
            input_schema (pa.Schema): The schema of the input data to validate.

        Raises:
            DataFrameSchemaError: If there are differences between the required and input schemas.
        """
        if differences := compare_schemas(required_schema, input_schema):
            raise DataFrameSchemaError(str(differences))

    def _verify_parquet_file(self, file_path: str) -> None:
        """
        Validates a single Parquet file for format and readability.

        Args:
            file_path (str): The path of the file to validate.

        Raises:
            CorruptedPathError: If the file is not a Parquet file or cannot be read properly.
        """
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
        """
        Moves invalid files to a designated error path.

        Args:
            invalid_blobs (List[Dict[str, str]]): A list of invalid blobs containing file paths and error messages.
        """
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
        """
        Handles invalid files by sorting and grouping them based on their error messages.

        Args:
            not_valid_blob (List[Dict[str, str]]): A list of invalid blobs containing file paths and error messages.
        """
        sorted_list = sorted(not_valid_blob, key=lambda item: item["error"])
        self._validation_resume = [
            {"message": key, "files": [item["path"] for item in items]}
            for key, items in itertools.groupby(sorted_list, key=lambda item: item["error"])
        ]

        self._error_context.errors["dataProcessing"] = str(self._validation_resume)

    def _get_schema_from_temp_copy(self, file_path: str) -> pa.Schema:
        """
        Retrieves the PyArrow schema from a temporary copy of the file.

        Args:
            file_path (str): The path of the file to retrieve the schema from.

        Returns:
            pa.Schema: The schema of the file.
        """
        with tempfile.TemporaryDirectory(prefix=os.path.basename(__file__)) as temp_dir:
            temp_file_path = os.path.join(temp_dir, os.path.basename(file_path))
            self._storage_client.copy([file_path], temp_dir)

            return get_pyarrow_schema(temp_file_path)

    def _validate_file(self, path: str) -> Dict[str, str] | None:
        """
        Validates a single file for format, readability, and schema.

        Args:
            path (str): The path of the file to validate.

        Returns:
            Dict[str, str] | None: A dictionary containing error details if the file is invalid, None otherwise.
        """
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
        """
        Executes the validation process for the set of files to be validated.

        Logs information about the validation process and handles any invalid files found during validation.
        """
        self._logger.info("Checking integrity of the data...")

        self._invalid_blobs = list(self._get_invalid_blobs(self._objects_to_validate))

        if self._invalid_blobs:
            self._handle_invalid_files(self._invalid_blobs)
            self._move_invalid_files(self._invalid_blobs)
        else:
            self._logger.info("Files validated successfully")

import json
from typing import List, Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from bubbaloo.services.pipeline import PipelineState
from bubbaloo.utils.functions.validation_csv_helper import validate_csv_params
from bubbaloo.utils.interfaces.pipeline_logger import ILogger
from bubbaloo.utils.interfaces.services_validation import IValidation
from bubbaloo.utils.interfaces.storage_client import IStorageManager


class CSV(IValidation):
    """Handles CSV file validation within a Spark-based data processing pipeline.

    This class provides functionality to validate CSV files against specified
    schema and format constraints. It integrates with Spark for reading files,
    a storage manager for file operations, and a pipeline logger for logging.

    Attributes:
        _params (dict): Parameters required for CSV validation.
        _objects_to_validate (List[str]): List of file paths to validate.
        _read_options (Dict[str, str]): Read options for Spark DataFrame.
        _spark_schema (StructType): Expected Spark DataFrame schema for validation.
        _spark (SparkSession): Spark session instance for data processing.
        _logger (ILogger): Logger interface for logging information and errors.
        _storage_client (IStorageManager): Interface for storage operations.
        _error_context (PipelineState): Context object for tracking pipeline errors.
        _error_path (str): File path to store invalid files.
        _format (str): Expected file format (default is 'csv').
        _invalid_files (List[str] | None): List of invalid file paths.
        _validation_resume (List[Dict[str, str]] | str): Summary of validation results.
        _error_message (List[Dict[str, str]]): Error messages for invalid files.

    Methods:
        invalid_files: Returns a list of invalid file paths.
        validation_resume: Returns a summary of the validation results.
        _check_file_extension: Validates the file extensions of the given file paths.
        _check_file_structure: Validates the file structure against the expected schema.
        _get_invalid_files: Identifies invalid files based on extension and structure.
        _handle_invalid_files: Processes and sorts the list of invalid files.
        _move_invalid_files: Moves invalid files to a specified error path.
        execute: Executes the validation process for the given file paths.
    """

    def __init__(self, **kwargs):
        """Initializes the CSV validation class with necessary parameters.

        Args:
            kwargs: A dictionary of parameters for CSV file validation.
        """

        self._params = validate_csv_params(kwargs)
        self._objects_to_validate: List[str] = self._params.get("objects_to_validate")
        self._read_options: Dict[str, str] = self._params.get("read_options", {})
        self._spark_schema: StructType = self._params.get("spark_schema")
        self._spark: SparkSession = self._params.get("spark")
        self._logger: ILogger = self._params.get("logger")
        self._storage_client: IStorageManager = self._params.get("storage_client")
        self._error_context: PipelineState = self._params.get("context")
        self._error_path: str = self._params.get("error_path")
        self._format = "csv"
        self._invalid_files: List[str] | None = None
        self._validation_resume: List[Dict[str, str]] | str = []
        self._error_message: List[Dict[str, str]] = []

    @property
    def invalid_files(self) -> List[str] | None:
        """Provides a list of invalid file paths identified during validation.

        Returns:
            List[str] | None: List of paths for invalid files or None if no invalid files.
        """
        return self._invalid_files

    @property
    def validation_resume(self) -> List[Dict[str, str]] | str:
        """Provides a summary of the validation results.

        Returns:
            List[Dict[str, str]] | str: Summary of validation results or an empty string.
        """
        return self._validation_resume

    def _check_file_extension(self, file_paths: List[str]) -> List[str]:
        """Validates the file extensions of the given file paths.

        Args:
            file_paths (List[str]): List of file paths to validate.

        Returns:
            List[str]: List of file paths with incorrect extensions.
        """
        malformed_files = [
            file for file in file_paths if not file.endswith(f".{self._format}")
        ]

        if malformed_files:
            self._error_message.append({
                "message": "They are not CSV files",
                "files": list(malformed_files)
            })

        return malformed_files

    def _check_file_structure(self, file_paths: List[str]) -> List[str]:
        """Validates the file structure against the expected schema.

        Args:
            file_paths (List[str]): List of file paths to validate.

        Returns:
            List[str]: List of file paths with structural issues.
        """
        self._read_options["mode"] = "FAILFAST"

        malformed_files = []
        schema_mismatch = []
        unreadable_csv = []

        for file in file_paths:
            try:
                df = (
                    self._spark
                    .read
                    .options(**self._read_options)
                    .schema(self._spark_schema)
                    .format(self._format)
                    .load(file)
                )
                df.first()
            except Exception as e:
                if "cannot be cast to" in str(e):
                    schema_mismatch.append(file)
                else:
                    unreadable_csv.append(file)
                malformed_files.append(file)

        if unreadable_csv:
            self._error_message.append({
                "message": "Error while reading file",
                "files": list(unreadable_csv)
            })

        if schema_mismatch:
            self._error_message.append({
                "message": "Schema does not match",
                "files": list(schema_mismatch)
            })

        return malformed_files

    def _get_invalid_files(self, files: List[str]) -> List[str]:
        """Identifies invalid files based on extension and structure.

        Args:
            files (List[str]): List of file paths to validate.

        Returns:
            List[str]: List of invalid file paths.
        """
        malformed_files = self._check_file_extension(files)

        files_to_validate = [file for file in files if file not in malformed_files]

        malformed_files.extend(self._check_file_structure(files_to_validate))

        return malformed_files

    def _handle_invalid_files(self, invalid_files: List[Dict[str, str]]) -> None:
        """Processes and sorts the list of invalid files.

        Args:
            invalid_files (List[Dict[str, str]]): List of dictionaries containing invalid file information.
        """
        self._validation_resume = sorted(invalid_files, key=lambda item: item["message"])
        self._error_context.errors["dataProcessing"] = str(self._validation_resume)

    def _move_invalid_files(self, malformed_files: List[str]) -> None:
        """Moves invalid files to a specified error path.

        Args:
            malformed_files (List[str]): List of paths for malformed files.
        """
        self._logger.error(
            "Some corrupted files found: \n"
            f"{json.dumps(self._error_context.errors['dataProcessing'])} \n"
            "Corrupted files will not be processed, check these files in storage."
        )

        self._storage_client.move(malformed_files, self._error_path)

        self._logger.info(
            f"Corrupted files have been moved to the following location in storage: \
            '{self._error_path}'."
        )

    def execute(self):
        """Executes the validation process for the given file paths.

        Validates file extensions and structures, handles invalid files,
        and logs the validation process.
        """
        self._logger.info("Checking integrity of the data...")

        self._invalid_files = self._get_invalid_files(self._objects_to_validate)

        if self._invalid_files:
            self._handle_invalid_files(self._error_message)
            self._move_invalid_files(self._invalid_files)
        else:
            self._logger.info("Files validated successfully")

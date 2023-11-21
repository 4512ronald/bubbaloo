import json


class CapturedException(Exception):
    """Base class for exceptions that capture other exceptions."""

    def __init__(self, message: str, source: str = None, error: Exception = None):
        self.source = source
        self.original_error = error
        self.message = message if source is None and error is None else f"Error at '{source}': {message}"
        super().__init__(self.message)

    def to_dict(self):
        """Converts the exception to a dictionary."""
        return {
            "message": self.message,
            "source": self.source,
            "original_error": self.original_error.to_dict()
            if isinstance(self.original_error, CapturedException)
            else str(self.original_error)
        }

    def __str__(self):
        """Converts the exception to a JSON string."""
        return json.dumps(self.to_dict())


class StageNotDefinedError(CapturedException):
    """Exception raised when a stage has not been defined in the pipeline."""


class ExecutionError(CapturedException):
    """Exception raised when there is an error during the execution of a pipeline stage."""


class BuilderStrategyNotFoundError(CapturedException):
    """Exception raised when the builder strategy cannot be found."""


class InvalidSourceError(CapturedException):
    """Exception raised when an invalid source is set."""


class InvalidEntityError(CapturedException):
    """Exception raised when an invalid entity is set."""


class PipelineCreationError(CapturedException):
    """Exception raised when an error occurs during pipeline creation."""


class DataReadError(CapturedException):
    """Exception raised when an error occurs while reading the data."""


class DataTransformError(CapturedException):
    """Exception raised when an error occurs while transform the data."""


class DataLoadError(CapturedException):
    """Exception raised when an error occurs while load the data."""


class CreateTableError(CapturedException):
    """Exception raised when an error occurs while creating table."""


class DataFrameSchemaError(CapturedException):
    """raise this when there's a schema error"""


class CorruptedPathError(CapturedException):
    """raise this when there's a path error"""


class ValidationError(CapturedException):
    """raise this when there's a validation error"""

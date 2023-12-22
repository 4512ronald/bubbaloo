from typing import Any, Dict

from pyspark.sql import SparkSession

from bubbaloo.config import default_settings
from bubbaloo.services.pipeline import PipelineState, Config
from bubbaloo.errors.errors import ExecutionError
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


def get_default_params(module: object) -> Dict[str, Any]:
    """
    Retrieves default parameters from the default_settings module.

    Scans the default_settings module for any uppercase attribute names and
    constructs a dictionary with these attributes and their values. The keys
    in the dictionary are converted to lowercase.

    Returns:
        Dict[str, Any]: A dictionary containing default parameter values.
    """
    return {
        key.lower(): getattr(module, key)
        for key in dir(module)
        if key.isupper()
    }


def validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates and augments provided parameters with default values if necessary.

    Ensures that required parameters are present and of the correct type. If
    certain expected parameters are missing, this function attempts to fill
    them in with default values. Raises a ValueError if a required parameter
    is missing or if a parameter is of an incorrect type.

    Args:
        params (Dict[str, Any]): The parameters to validate and augment.

    Returns:
        Dict[str, Any]: The validated and possibly augmented parameters.

    Raises:
        ValueError: If a required parameter is missing or of incorrect type.
    """
    default_params = get_default_params(default_settings)

    param_types = {
        'logger': ILogger,
        'spark': SparkSession,
        'context': PipelineState,
        'conf': Config,
        'pipeline_name': str,
    }

    if "conf" not in params:
        raise ValueError("Missing required 'conf' parameter")

    for param, expected_type in param_types.items():

        if param in params and not isinstance(params[param], expected_type):
            raise TypeError(f"Expected '{param}' of type {expected_type.__name__}")

        if param not in params:
            params[param] = default_params[param]

    return params


def raise_error(logger: ILogger, source: str, message: str, error: Exception) -> None:
    """
    Logs an error message and raises a custom ExecutionError.

    This function logs the provided message as an error using the given logger
    and then raises an ExecutionError with the message, source, and original
    error.

    Args:
        logger (ILogger): The logger to use for logging the error message.
        source (str): The source identifier of the error.
        message (str): The error message to log and include in the raised exception.
        error (Exception): The original exception that caused the error.

    Raises:
        ExecutionError: A custom exception encapsulating the error details.
    """
    error_message = message
    logger.error(error_message)
    raise ExecutionError(error_message, source, error) from error

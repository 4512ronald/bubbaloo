from typing import Any, Dict

from pyspark.sql import SparkSession

from bubbaloo.config import default_settings
from bubbaloo.services.pipeline import PipelineState, Config
from bubbaloo.errors.errors import ExecutionError
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


def get_default_params() -> Dict[str, Any]:
    return {
        key.lower(): getattr(default_settings, key)
        for key in dir(default_settings)
        if key.isupper()
    }


def validate_params(params: Dict[str, Any]) -> Dict[str, Any]:

    default_params = get_default_params()

    param_types = {
        'logger': ILogger,
        'spark': SparkSession,
        'context': PipelineState,
        'conf': Config
    }

    if "conf" not in params:
        raise ValueError("Missing required 'conf' parameter")

    for param, expected_type in param_types.items():

        if param in params and not isinstance(params[param], expected_type):
            raise ValueError(f"Expected '{param}' of type {expected_type.__name__}")

        if param not in params:
            params[param] = default_params[param]

    return params


def raise_error(logger: ILogger, source: str, message: str, error: Exception) -> None:
    error_message = message
    logger.error(error_message)
    raise ExecutionError(error_message, source, error) from error

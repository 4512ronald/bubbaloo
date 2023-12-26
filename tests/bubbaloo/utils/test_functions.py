import textwrap
from unittest.mock import Mock

import pytest
import importlib.util

from pyspark.sql import SparkSession

from bubbaloo.errors.errors import ExecutionError
from bubbaloo.services.local import Logger
from bubbaloo.services.pipeline import PipelineState
from bubbaloo.utils.functions.pipeline_stage_helper import validate_params, raise_error, get_default_params
from bubbaloo.utils.functions.pipeline_orchestration_helper import get_error


class TestPipelineOrchestrationHelper:
    """
    A test class for verifying the functionality of the Pipeline Orchestration Helper.

    This class tests various utility functions used in pipeline orchestration, such as error extraction from exceptions.
    """
    def test_get_error_with_json_exception(self):
        """
        Test the extraction of error information from a JSON formatted exception.

        This test verifies that when an exception containing a JSON formatted error message is raised, the function
        correctly extracts and returns the error in JSON format.
        """
        exception = Exception('{"error": "Test error"}')
        result = get_error(exception)
        assert result == {"error": "Test error"}

    def test_get_error_with_plain_text_exception(self):
        """
        Test the extraction of error information from a plain text exception.

        This test checks that when an exception containing a plain text error message is raised, the function correctly
         formats and returns the error as a JSON object.
        """
        exception = Exception('Test error')
        result = get_error(exception)
        assert result == {"error": "Test error"}

    def test_get_error_with_non_json_exception(self):
        """
        Test the extraction of error information from a malformed JSON exception.

        This test ensures that when an exception with a malformed JSON error message is raised, the function returns
        the error message as-is in a JSON object.
        """
        exception = Exception('{malformed json}')
        result = get_error(exception)
        assert result == {"error": "{malformed json}"}


class TestPipelineStageHelper:
    """
    A test class for verifying the functionality of the Pipeline Stage Helper.

    This class tests functions related to default parameter retrieval, parameter validation, and error raising within
    pipeline stages.
    """

    @pytest.fixture
    def tmp_default_settings_path(self, tmp_path):
        """
        A pytest fixture that creates a temporary file simulating a module containing default settings.

        Args:
            tmp_path (Path): A fixture that provides a temporary directory.

        Returns:
            str: The path to the created temporary default settings file.
        """
        conf = tmp_path / "config"
        conf.mkdir()

        path = conf / "default_settings.py"

        code = textwrap.dedent("""
            from bubbaloo.services.local.logger import Logger
            from bubbaloo.services.pipeline.get_spark import GetSpark
            from bubbaloo.services.pipeline.state import PipelineState
            
            LOGGER = Logger()
            SPARK = GetSpark()
            CONTEXT = PipelineState()
            PIPELINE_NAME = "Default Pipeline"
        """)

        path.write_text(code)
        return str(path)

    def test_get_default_params(self, tmp_default_settings_path):
        """
        Test the retrieval of default parameters from a settings module.

        This test verifies that the function correctly retrieves and returns default parameters defined in a given
        module.

        Args:
            tmp_default_settings_path (str): The path to the temporary default settings file.
        """

        spec = importlib.util.spec_from_file_location("default_settings", tmp_default_settings_path)
        default_settings = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(default_settings)

        params = {
            'logger': Logger,
            'spark': SparkSession,
            'context': PipelineState,
            'pipeline_name': str
        }

        result = get_default_params(default_settings)

        for key, value in params.items():
            assert isinstance(result[key], value)

    def test_validate_params(self, logger, spark, state, config):
        """
        Test the validation of provided parameters against expected types.

        This test checks that the function correctly validates given parameters and returns them when they match the
        expected types.

        Args:
            logger (Logger): The logger object.
            spark (SparkSession): The SparkSession object.
            state (PipelineState): The state object.
            config (Config): The configuration object.
        """
        params = {
            'logger': logger,
            'spark': spark,
            'context': state,
            'conf': config,
            'pipeline_name': 'test_pipeline'
        }
        result = validate_params(params)
        assert result == params

    def test_raise_error(self):
        """
        Test the raising of a custom ExecutionError.

        This test verifies that the function correctly raises an ExecutionError with a specified message and logs the
        error.
        """
        logger = Mock()
        with pytest.raises(ExecutionError):
            raise_error(logger, 'test_source', 'test_message', Exception('test_error'))
        logger.error.assert_called_once_with('test_message')

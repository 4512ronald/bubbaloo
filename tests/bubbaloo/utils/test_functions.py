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
    def test_get_error_with_json_exception(self):
        exception = Exception('{"error": "Test error"}')
        result = get_error(exception)
        assert result == {"error": "Test error"}

    def test_get_error_with_plain_text_exception(self):
        exception = Exception('Test error')
        result = get_error(exception)
        assert result == {"error": "Test error"}

    def test_get_error_with_non_json_exception(self):
        exception = Exception('{malformed json}')
        result = get_error(exception)
        assert result == {"error": "{malformed json}"}


class TestPipelineStageHelper:

    @pytest.fixture
    def tmp_default_settings_path(self, tmp_path):
        config = tmp_path / "config"
        config.mkdir()

        path = config / "default_settings.py"

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
        logger = Mock()
        with pytest.raises(ExecutionError):
            raise_error(logger, 'test_source', 'test_message', Exception('test_error'))
        logger.error.assert_called_once_with('test_message')

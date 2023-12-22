import importlib
import re
import importlib.util
import sys
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StructField, StringType, TimestampType, StructType

from bubbaloo.pipeline.stages import Extract
from bubbaloo.services.local import Logger
from bubbaloo.orchestration import Orchestrator


@pytest.mark.usefixtures(
    "write_modules_entity_1",
    "write_modules_entity_2",
    "write_modules_entity_3",
    "write_modules_entity_4",
    "raw_data_parquet"
)
class TestOrchestratorInitialization:

    @pytest.fixture
    def path(self, flows_dir):
        return str(flows_dir)

    def test_initialization(self, path, config, state, logger, flows_to_execute):
        orchestrator = Orchestrator(path, conf=config, context=state, logger=logger)
        assert orchestrator._path_to_flows == path
        assert orchestrator._flows_to_execute == []

        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        assert orchestrator._flows_to_execute == flows_to_execute

    def test_get_module_names_from_package(self, path, config, state, logger, entity1_dir, entity4_dir):
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        assert set(orchestrator._get_module_names_from_package(entity1_dir)) == {"extract", "transform", "load"}

        empty_dir = entity4_dir
        orchestrator = Orchestrator(path, ["entity4"], conf=config, context=state, logger=logger)
        assert orchestrator._get_module_names_from_package(empty_dir) == []

        (entity1_dir / "not_a_module.txt").touch()
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        assert set(orchestrator._get_module_names_from_package(entity1_dir)) == {"extract", "transform", "load"}

    def test_get_pipeline_phases_from_module(self, path, config, state, logger, entity1_dir):
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        spec = importlib.util.spec_from_file_location("extract", f"{entity1_dir}/extract.py")

        module = importlib.util.module_from_spec(spec)

        spec.loader.exec_module(module)

        pipeline_phases = orchestrator._get_pipeline_phases_from_module(module)
        assert len(pipeline_phases) == 1
        assert pipeline_phases[0][0] == "ExtractStage"
        assert isinstance(pipeline_phases[0][1], Extract)

    def test_get_pipeline_phases_from_package(self, path, config, state, logger):
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        sys.path.append('/'.join(path.split("/")[0:-1]))
        pipeline_phases = orchestrator._get_pipeline_phases_from_package("entity1")
        assert len(pipeline_phases) == 3
        assert any(phase[0] == "ExtractStage" for phase in pipeline_phases)
        assert any(phase[0] == "TransformStage1" for phase in pipeline_phases)
        assert any(phase[0] == "LoadStage" for phase in pipeline_phases)

        pipeline_phases = orchestrator._get_pipeline_phases_from_package("entity4")
        assert pipeline_phases == []


@pytest.mark.usefixtures(
    "write_modules_entity_1",
    "write_modules_entity_2",
    "write_modules_entity_3",
    "write_modules_entity_4",
    "raw_data_parquet"
)
class TestOrchestratorExecution:

    @pytest.fixture
    def path(self, flows_dir):
        return str(flows_dir)

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return StructType(
            [
                StructField("ComunicacionID", StringType(), True),
                StructField("UserID", StringType(), True),
                StructField("Fecha", StringType(), True),
                StructField("Estado", StringType(), True),
                StructField("descripcion", StringType(), True),
            ]
        )

    @pytest.fixture(scope="class")
    def expected_schema_chained_transforms(self):
        return StructType(
            [
                StructField("ComunicacionID", StringType(), True),
                StructField("UserID", StringType(), True),
                StructField("Fecha", StringType(), True),
                StructField("Estado", StringType(), True),
                StructField("descripcion", StringType(), True),
                StructField("fecha_ingreso", TimestampType(), True),
            ]
        )

    @pytest.fixture(scope="class")
    def expected_schema_without_transform(self):
        return StructType(
            [
                StructField("ComunicacionID", StringType(), True),
                StructField("UserID", StringType(), True),
                StructField("Fecha", StringType(), True),
                StructField("Estado", StringType(), True),
            ]
        )

    def test_execution_with_one_flow(self, spark, path, config, state, logger, flows_to_execute,
                                     expected_schema, expected_schema_without_transform):
        sys.path.append('/'.join(path.split("/")[0:-1]))
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()

        df_entity1 = spark.read.parquet(config.trusted_path)

        assert df_entity1.schema == expected_schema
        assert df_entity1.count() > 0
        assert len(df_entity1.take(1)) == 1

    def test_execution_with_multiple_flows(self, spark, path, config, state, logger, flows_to_execute,
                                           expected_schema, expected_schema_chained_transforms):
        sys.path.append('/'.join(path.split("/")[0:-1]))
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()

        df_entity1 = spark.read.parquet(config.trusted_path)

        assert df_entity1.schema == expected_schema
        assert df_entity1.count() > 0
        assert len(df_entity1.take(1)) == 1

        df_entity2 = spark.read.parquet(config.trusted_path2)

        assert df_entity2.schema == expected_schema_chained_transforms
        assert df_entity2.count() > 0
        assert len(df_entity2.take(1)) == 1


@pytest.mark.usefixtures(
    "write_modules_entity_1",
    "write_modules_entity_2",
    "write_modules_entity_3",
    "write_modules_entity_4",
    "raw_data_parquet"
)
class TestOrchestratorSideEffectsAndState:

    @pytest.fixture
    def path(self, flows_dir):
        return str(flows_dir)

    def test_state_updates_correctly_for_each_flow(self, path, config, state, logger, flows_to_execute):
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()
        assert orchestrator.resume[-1]["entity"] == flows_to_execute[-1]

    def test_state_reset_after_each_flow(self, path, config, state, logger, flows_to_execute):
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()
        assert orchestrator._context.errors["PipelineExecution"] == "No errors"

    def test_errors_are_logged_correctly_in_state(self, path, config, state, logger, flows_to_execute):
        orchestrator = Orchestrator(path, ["entity3"], conf=config, context=state, logger=logger)
        orchestrator.execute()
        assert orchestrator.resume[0]["errors"]["PipelineExecution"] != "No errors"  # noqa


@pytest.mark.usefixtures(
    "write_modules_entity_1",
    "write_modules_entity_2",
    "write_modules_entity_3",
    "write_modules_entity_4",
    "raw_data_parquet"
)
class TestOrchestratorLoggerHandling:

    @pytest.fixture
    def logger_mock(self):
        logger = MagicMock(spec=Logger)
        return logger

    @pytest.fixture
    def path(self, flows_dir):
        return str(flows_dir)

    def test_start_and_end_execution_logging(self, path, config, state, logger_mock, flows_to_execute):
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger_mock)
        orchestrator.execute()

        logger_mock.info.assert_any_call(f"Executing pipeline for flow: {flows_to_execute[0]}")
        logger_mock.info.assert_any_call(f"The ETL: {flows_to_execute[-1]} has finished")

    def test_error_logging(self, path, config, state, logger_mock, flows_to_execute_with_errors):
        orchestrator = Orchestrator(path, flows_to_execute_with_errors, conf=config, context=state, logger=logger_mock)
        orchestrator.execute()

        error_log_call_args = logger_mock.error.call_args_list
        pattern = re.compile(r"Error executing pipeline for flow")

        assert any(pattern.search(str(call_args)) for call_args in error_log_call_args)

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
    """
    A test class for Orchestrator initialization and related functionalities.

    This class uses several fixtures to set up the necessary environment and dependencies for testing the
    Orchestrator. It includes tests for initialization, module name extraction, and pipeline phase retrieval from
    modules and packages.
    """

    @pytest.fixture
    def path(self, flows_dir):
        """
        A pytest fixture to provide the path to the flows directory.

        Args:
            flows_dir (Path): The base directory for flow entities.

        Returns:
            str: The string representation of the path to the flows directory.
        """
        return str(flows_dir)

    def test_initialization(self, path, config, state, logger, flows_to_execute):
        """
        Test the initialization of the Orchestrator.

        This test verifies that the Orchestrator is correctly initialized with the provided path and other
        parameters. It also checks that the list of flows to execute is set correctly.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
        """
        orchestrator = Orchestrator(path, conf=config, context=state, logger=logger)
        assert orchestrator._path_to_flows == path
        assert orchestrator._flows_to_execute == []

        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        assert orchestrator._flows_to_execute == flows_to_execute

    def test_get_module_names_from_package(self, path, config, state, logger, entity1_dir, entity4_dir):
        """
        Test retrieval of module names from a package.

        This test checks whether the Orchestrator correctly retrieves the names of modules from a given package. It
        specifically tests with 'entity1' and 'entity4' to verify different scenarios, including the presence of
        non-module files.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            entity1_dir (Path): Path to the directory for 'entity1'.
            entity4_dir (Path): Path to the directory for 'entity4'.
        """
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        assert set(orchestrator._get_module_names_from_package(entity1_dir)) == {"extract", "transform", "load"}

        orchestrator = Orchestrator(path, ["entity4"], conf=config, context=state, logger=logger)
        assert orchestrator._get_module_names_from_package(entity4_dir) == []

        (entity1_dir / "not_a_module.txt").touch()
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        assert set(orchestrator._get_module_names_from_package(entity1_dir)) == {"extract", "transform", "load"}

    def test_get_pipeline_phases_from_module(self, path, config, state, logger, entity1_dir):
        """
        Test extraction of pipeline phases from a module.

        This test verifies the Orchestrator's capability to extract pipeline phases from a specified module. It tests
        this functionality using the 'entity1' directory.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            entity1_dir (Path): Path to the directory for 'entity1'.
        """
        orchestrator = Orchestrator(path, ["entity1"], conf=config, context=state, logger=logger)
        spec = importlib.util.spec_from_file_location("extract", f"{entity1_dir}/extract.py")

        module = importlib.util.module_from_spec(spec)

        spec.loader.exec_module(module)

        pipeline_phases = orchestrator._get_pipeline_phases_from_module(module)
        assert len(pipeline_phases) == 1
        assert pipeline_phases[0][0] == "ExtractStage"
        assert isinstance(pipeline_phases[0][1], Extract)

    def test_get_pipeline_phases_from_package(self, path, config, state, logger):
        """
        Test extraction of pipeline phases from a package.

        This test checks the Orchestrator's ability to extract all pipeline phases from a specified package. It tests
        with both 'entity1' and 'entity4' to verify different outcomes based on the package contents.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
        """
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
    """
    A test class for verifying the execution of the Orchestrator.

    This class tests the Orchestrator's ability to execute data processing flows. It uses various fixtures to set up
    the necessary environment and dependencies and includes tests for different scenarios of data flow execution.
    """

    @pytest.fixture
    def path(self, flows_dir):
        """
        A pytest fixture to provide the path to the flows directory.

        Args:
            flows_dir (Path): The base directory for flow entities.

        Returns:
            str: The string representation of the path to the flows directory.
        """
        return str(flows_dir)

    @pytest.fixture(scope="class")
    def expected_schema(self):
        """
        A pytest fixture that provides the expected schema after a single transformation.

        Returns: StructType: A PySpark StructType representing the expected schema of the DataFrame after a single
        transformation.
        """
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
        """
        A pytest fixture that provides the expected schema after multiple transformations.

        Returns: StructType: A PySpark StructType representing the expected schema of the DataFrame after applying
        chained transformations.
        """
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
        """
        A pytest fixture that provides the expected schema without any transformations.

        Returns: StructType: A PySpark StructType representing the expected schema of the DataFrame with no
        transformations applied.
        """
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
        """
        Test the execution of the Orchestrator with a single data flow.

        This test verifies that the Orchestrator executes a single data flow correctly and produces the expected
        schema and data count.

        Args:
            spark (SparkSession): The SparkSession instance.
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
            expected_schema (StructType): The expected schema after transformation.
            expected_schema_without_transform (StructType): The expected schema without transformation.
        """
        sys.path.append('/'.join(path.split("/")[0:-1]))
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()

        df_entity1 = spark.read.parquet(config.trusted_path)

        assert df_entity1.schema == expected_schema
        assert df_entity1.count() > 0
        assert len(df_entity1.take(1)) == 1

    def test_execution_with_multiple_flows(self, spark, path, config, state, logger, flows_to_execute,
                                           expected_schema, expected_schema_chained_transforms):
        """
        Test the execution of the Orchestrator with multiple data flows.

        This test checks whether the Orchestrator correctly executes multiple data flows and produces DataFrames with
        the expected schemas and data counts.

        Args:
            spark (SparkSession): The SparkSession instance.
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
            expected_schema (StructType): The expected schema after a single transformation.
            expected_schema_chained_transforms (StructType): The expected schema after multiple transformations.
        """
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
    """
    A test class for verifying the side effects and state management of the Orchestrator.

    This class tests the Orchestrator's ability to handle state changes and side effects during the execution of data
    flows. It ensures that the state is updated and reset correctly and that errors are logged appropriately.
    """

    @pytest.fixture
    def path(self, flows_dir):
        """
        A pytest fixture to provide the path to the flows directory.

        Args:
            flows_dir (Path): The base directory for flow entities.

        Returns:
            str: The string representation of the path to the flows directory.
        """
        return str(flows_dir)

    def test_state_updates_correctly_for_each_flow(self, path, config, state, logger, flows_to_execute):
        """
        Test that the Orchestrator's state updates correctly for each flow.

        This test ensures that the Orchestrator correctly updates its state after executing each flow, specifically
        verifying that the last executed entity is recorded correctly.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
        """
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()
        assert orchestrator.resume[-1]["entity"] == flows_to_execute[-1]

    def test_state_reset_after_each_flow(self, path, config, state, logger, flows_to_execute):
        """
        Test that the Orchestrator's state is reset after each flow.

        This test checks that the Orchestrator resets its internal state after executing each flow, particularly
        verifying the absence of errors in the context.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
        """
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger)
        orchestrator.execute()
        assert orchestrator._context.errors["PipelineExecution"] == "No errors"

    def test_errors_are_logged_correctly_in_state(self, path, config, state, logger, flows_to_execute):
        """
        Test that errors are logged correctly in the Orchestrator's state.

        This test ensures that the Orchestrator correctly logs any errors encountered during the execution of a flow,
        especially when executing a flow known to cause errors.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger (Logger): Logger object.
            flows_to_execute (list): List of flows to be executed.
        """
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
    """
    A test class for verifying the logger handling of the Orchestrator.

    This class tests the Orchestrator's ability to log information and errors during the execution of data flows. It
    ensures that start, end, and error events are logged correctly.
    """

    @pytest.fixture
    def logger_mock(self):
        """
        A pytest fixture that provides a mock logger.

        Returns:
            MagicMock: A mock object spec had as a Logger.
        """
        logger = MagicMock(spec=Logger)
        return logger

    @pytest.fixture
    def path(self, flows_dir):
        """
        A pytest fixture to provide the path to the flows directory.

        Args:
            flows_dir (Path): The base directory for flow entities.

        Returns:
            str: The string representation of the path to the flows directory.
        """
        return str(flows_dir)

    def test_start_and_end_execution_logging(self, path, config, state, logger_mock, flows_to_execute):
        """
        Test logging at the start and end of execution in the Orchestrator.

        This test verifies that the Orchestrator correctly logs the beginning and completion of each flow execution,
        ensuring appropriate logging messages are generated.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger_mock (MagicMock): Mock logger object.
            flows_to_execute (list): List of flows to be executed.
        """
        orchestrator = Orchestrator(path, flows_to_execute, conf=config, context=state, logger=logger_mock)
        orchestrator.execute()

        logger_mock.info.assert_any_call(f"Executing pipeline for flow: {flows_to_execute[0]}")
        logger_mock.info.assert_any_call(f"The ETL: {flows_to_execute[-1]} has finished")

    def test_error_logging(self, path, config, state, logger_mock, flows_to_execute_with_errors):
        """
        Test error logging in the Orchestrator.

        This test checks that the Orchestrator correctly logs errors during the execution of flows, particularly when
        executing flows known to cause errors.

        Args:
            path (str): Path to the flows directory.
            config (Config): Configuration object.
            state (State): State object.
            logger_mock (MagicMock): Mock logger object.
            flows_to_execute_with_errors (list): List of flows to be executed, including those expected to produce
                                                 errors.
        """
        orchestrator = Orchestrator(path, flows_to_execute_with_errors, conf=config, context=state, logger=logger_mock)
        orchestrator.execute()

        error_log_call_args = logger_mock.error.call_args_list
        pattern = re.compile(r"Error executing pipeline for flow")

        assert any(pattern.search(str(call_args)) for call_args in error_log_call_args)

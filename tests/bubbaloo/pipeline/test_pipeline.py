import pytest
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from bubbaloo.errors.errors import ExecutionError
from bubbaloo.pipeline.pipeline import Pipeline
from bubbaloo.pipeline.stages import Extract, Transform, Load
from bubbaloo.services.pipeline.config import Config
from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.services.local import Logger
from pyspark.sql import SparkSession


class TestPipelineInitialization:
    """
    A test class for verifying the initialization of the Pipeline.

    This class contains tests to ensure the proper initialization of the Pipeline class with various configurations
    and parameter validations. It includes tests for correct instantiation, handling of invalid parameters,
    and default behavior when minimal parameters are provided.
    """
    def test_pipeline_initialization(self, config, spark, logger, state, measure):
        """
        Test the correct initialization of the Pipeline.

        This test verifies that the Pipeline is correctly initialized with given parameters and that the created
        pipeline instance has the expected attributes and types.

        Args:
            config (Config): The configuration object.
            spark (SparkSession): The SparkSession instance.
            logger (Logger): The logger object.
            state (PipelineState): The state object for the pipeline.
            measure (Measure): The measurement object for performance metrics.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        assert pipeline.name == "test_pipeline"
        assert isinstance(pipeline._logger, Logger)
        assert isinstance(pipeline._spark, SparkSession)
        assert isinstance(pipeline._context, PipelineState)
        assert isinstance(pipeline._conf, Config)

    def test_pipeline_invalid_parameters(self, config, spark, logger, state, measure):
        """
        Test the Pipeline initialization with invalid parameters.

        This test ensures that the Pipeline raises appropriate exceptions when initialized with invalid parameters
        for logger, spark, context, and configuration.

        Args:
            config (Config): The configuration object.
            spark (SparkSession): The SparkSession instance.
            logger (Logger): The logger object.
            state (PipelineState): The state object for the pipeline.
            measure (Measure): The measurement object for performance metrics.
        """
        with pytest.raises(TypeError):
            Pipeline(
                pipeline_name="test_pipeline",
                logger="invalid_logger",
                spark=spark,
                context=state,
                conf=config,
                measure=measure
            )

        with pytest.raises(TypeError):
            Pipeline(
                pipeline_name="test_pipeline",
                logger=logger,
                spark="invalid_spark",
                context=state,
                conf=config,
                measure=measure
            )

        with pytest.raises(TypeError):
            Pipeline(
                pipeline_name="test_pipeline",
                logger=logger,
                spark=spark,
                context="invalid_context",
                conf=config,
                measure=measure
            )

        with pytest.raises(TypeError):
            Pipeline(
                pipeline_name="test_pipeline",
                logger=logger,
                spark=spark,
                context=state,
                conf="invalid_conf",
                measure=measure
            )

        with pytest.raises(ValueError):
            Pipeline(
                pipeline_name="test_pipeline",
                logger=logger,
                spark=spark,
                context=state,
                measure=measure
            )

    def test_pipeline_with_only_conf(self, config):
        """
        Test the Pipeline initialization with only the configuration parameter.

        This test verifies that the Pipeline can be correctly initialized with only the configuration parameter and
        that default values are used for other attributes.

        Args:
            config (Config): The configuration object.
        """
        pipeline = Pipeline(conf=config)

        assert pipeline.name is not None
        assert pipeline._logger is not None
        assert pipeline._spark is not None
        assert pipeline._context is not None
        assert isinstance(pipeline._conf, Config)

    def test_name(self, config, spark, logger, state, measure):
        """
        Test the assignment of the name attribute in the Pipeline.

        This test checks that the 'name' attribute of the Pipeline is correctly assigned and matches the expected
        value when provided during initialization.

        Args:
            config (Config): The configuration object.
            spark (SparkSession): The SparkSession instance.
            logger (Logger): The logger object.
            state (PipelineState): The state object for the pipeline.
            measure (Measure): The measurement object for performance metrics.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        assert pipeline.name == "test_pipeline"


class TestPipelineStages:
    """
    A test class for verifying the management and validation of stages in the Pipeline.

    This class includes tests to check the correct addition of various stages to the Pipeline and to ensure that
    invalid stage instances are properly rejected.
    """
    def test_adding_stages(self, config, spark, logger, state, measure, batch_extract, batch_transform_1, batch_load):
        """
        Test the addition of stages to the Pipeline.

        This test verifies that stages (extract, transform, load) can be correctly added to the Pipeline and that the
        Pipeline correctly recognizes and stores these stages.

        Args:
            config (Config): The configuration object.
            spark (SparkSession): The SparkSession instance.
            logger (Logger): The logger object.
            state (PipelineState): The state object for the pipeline.
            measure (Measure): The measurement object for performance metrics.
            batch_extract (ExtractStage): The extract stage fixture.
            batch_transform_1 (TransformStage): The transform stage fixture.
            batch_load (LoadStage): The load stage fixture.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform", batch_transform_1),
            ("load", batch_load)
        ])

        assert pipeline._stages["extract"][0] == "extract"
        assert isinstance(pipeline._stages["extract"][1], Extract)
        assert pipeline._stages["transform"][0][0] == "transform"
        assert isinstance(pipeline._stages["transform"][0][1], Transform)
        assert pipeline._stages["load"][0] == "load"
        assert isinstance(pipeline._stages["load"][1], Load)

    def test_invalid_stage_instance(self, config, spark, logger, state, measure):
        """
        Test the addition of an invalid stage instance to the Pipeline.

        This test ensures that the Pipeline raises a TypeError when an attempt is made to add an invalid stage instance,
        such as a non-stage type object.

        Args:
            config (Config): The configuration object.
            spark (SparkSession): The SparkSession instance.
            logger (Logger): The logger object.
            state (PipelineState): The state object for the pipeline.
            measure (Measure): The measurement object for performance metrics.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        with pytest.raises(TypeError):
            pipeline.stages([("invalid_stage", "not a stage instance")])


@pytest.mark.usefixtures("raw_data_parquet")
class TestPipelineExecution:
    """
    A test class for verifying the execution of the Pipeline.

    This class includes tests for various execution scenarios of the Pipeline, including batch and streaming ETL
    processes, execution with multiple transforms, and error handling. It also tests the Pipeline's ability to reset
    after execution.
    """

    @pytest.fixture(scope="class")
    def expected_schema(self):
        """
        A pytest fixture that provides the expected schema after a single transformation.

        Returns:
            StructType: A PySpark StructType representing the expected schema of the DataFrame after a single
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

        Returns:
            StructType: A PySpark StructType representing the expected schema of the DataFrame after applying chained
            transformations.
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

        Returns:
            StructType: A PySpark StructType representing the expected schema of the DataFrame with no transformations
            applied.
        """
        return StructType(
            [
                StructField("ComunicacionID", StringType(), True),
                StructField("UserID", StringType(), True),
                StructField("Fecha", StringType(), True),
                StructField("Estado", StringType(), True),
            ]
        )

    def test_batch_successful_execution(self, config, spark, logger, state, measure, batch_extract, batch_transform_1,
                                        batch_load, expected_schema):
        """
        Test the successful execution of a batch ETL process in the Pipeline.

        This test verifies that the Pipeline correctly executes a batch ETL process with extract, transform, and load
        stages, and produces a DataFrame with the expected schema and data count.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_transform_1, batch_load: Stage fixtures for the Pipeline.
            expected_schema: The expected DataFrame schema post-execution.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform", batch_transform_1),
            ("load", batch_load)
        ])

        pipeline.execute()

        df = spark.read.parquet(config.trusted_path)

        assert df.schema == expected_schema
        assert df.count() > 0
        assert len(df.take(1)) == 1

    def test_streaming_etl_execution(self, config, spark, logger, state, measure, streaming_extract,
                                     streaming_transform, streaming_load, expected_schema):
        """
        Test the execution of a streaming ETL process in the Pipeline.

        This test checks whether the Pipeline can correctly execute a streaming ETL process and verifies that the output
        DataFrame matches the expected schema and data count.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            streaming_extract, streaming_transform, streaming_load: Stage fixtures for the Pipeline.
            expected_schema: The expected DataFrame schema post-execution.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", streaming_extract),
            ("transform", streaming_transform),
            ("load", streaming_load)
        ])

        pipeline.execute()

        df = spark.read.parquet(config.trusted_path)

        assert df.schema == expected_schema
        assert df.count() > 0
        assert len(df.take(1)) == 1

    def test_batch_etl_execution_with_multiple_transforms(self, config, spark, logger, state, measure,
                                                          batch_extract, batch_transform_1, batch_transform_2,
                                                          batch_load, expected_schema_chained_transforms):
        """
        Test the execution of a batch ETL process with multiple transform stages in the Pipeline.

        This test ensures that the Pipeline correctly executes a batch ETL process with multiple transforms and that
        the output DataFrame adheres to the expected schema.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_transform_1, batch_transform_2, batch_load: Stage fixtures for the Pipeline.
            expected_schema_chained_transforms: The expected DataFrame schema post-execution with chained transforms.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform1", batch_transform_1),
            ("transform2", batch_transform_2),
            ("load", batch_load)
        ])

        pipeline.execute()

        df = spark.read.parquet(config.trusted_path)

        assert df.schema == expected_schema_chained_transforms
        assert df.count() > 0
        assert len(df.take(1)) == 1

    def test_etl_execution_without_transform(self, config, spark, logger, state, measure, batch_extract, batch_load,
                                             expected_schema_without_transform):
        """
        Test the execution of an ETL process without a transform stage in the Pipeline.

        This test checks the Pipeline's ability to execute an ETL process with only extract and load stages, and
        verifies that the output DataFrame conforms to the expected schema.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_load: Stage fixtures for the Pipeline.
            expected_schema_without_transform: The expected DataFrame schema post-execution without any transform.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("load", batch_load)
        ])

        pipeline.execute()

        df = spark.read.parquet(config.trusted_path)

        assert df.schema == expected_schema_without_transform
        assert df.count() > 0
        assert len(df.take(1)) == 1

    def test_error_handling(self, config, spark, logger, state, measure, error_extract, error_transform, error_load):
        """
        Test the error handling during the execution of the Pipeline.

        This test verifies that the Pipeline correctly handles errors in any stage of execution by raising an
        appropriate ExecutionError exception.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            error_extract, error_transform, error_load: Stage fixtures that simulate errors.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        with pytest.raises(ExecutionError):
            pipeline.stages([
                ("extract", error_extract),
                ("transform", error_transform),
                ("load", error_load)
            ])

            pipeline.execute()

    def test_pipeline_reset(self, config, spark, logger, state, measure, batch_extract, batch_transform_1, batch_load):
        """
        Test the reset functionality of the Pipeline post-execution.

        This test checks that the Pipeline correctly resets its internal state and stages after execution, ensuring
        that it's ready for subsequent executions.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_transform_1, batch_load: Stage fixtures for the Pipeline.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform", batch_transform_1),
            ("load", batch_load)
        ])

        pipeline.execute()

        assert pipeline._pipeline is None
        assert pipeline._stages["extract"] is None
        assert pipeline._stages["transform"] == []
        assert pipeline._stages["load"] is None


class TestPipelineMethods:
    """
    A test class for verifying various methods and properties of the Pipeline.

    This class includes tests to verify the implementation and functionality of the Pipeline's `__str__` method,
    properties like `is_streaming` and `named_stages`, and the `name` property.
    """
    def test_str_method(self, config, spark, logger, state, measure, batch_extract, batch_transform_1, batch_load):
        """
        Test the string representation method of the Pipeline.

        This test verifies that the `__str__` method of the Pipeline class returns the expected string representation,
        including the name and a representation of the stages.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_transform_1, batch_load: Stage fixtures for the Pipeline.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform", batch_transform_1),
            ("load", batch_load)
        ])

        assert str(pipeline) == "name: test_pipeline, stages: {}"

    def test_is_streaming_property(self, config, spark, logger, state, measure, streaming_extract, streaming_transform,
                                   streaming_load):
        """
        Test the 'is_streaming' property of the Pipeline.

        This test verifies that the `is_streaming` property correctly identifies whether the Pipeline is set up for
         streaming data processing.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            streaming_extract, streaming_transform, streaming_load: Stage fixtures for the Pipeline.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", streaming_extract),
            ("transform", streaming_transform),
            ("load", streaming_load)
        ]).execute()

        assert pipeline.is_streaming is True

    def test_named_stages_property(self, config, spark, logger, state, measure, batch_extract, batch_transform_1,
                                   batch_load):
        """
        Test the 'named_stages' property of the Pipeline.

        This test checks that the `named_stages` property correctly lists the names of all stages added to the Pipeline.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
            batch_extract, batch_transform_1, batch_load: Stage fixtures for the Pipeline.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        pipeline.stages([
            ("extract", batch_extract),
            ("transform", batch_transform_1),
            ("load", batch_load)
        ]).execute()

        assert len(pipeline.named_stages) == 3
        assert "extract" in pipeline.named_stages
        assert "transform" in pipeline.named_stages
        assert "load" in pipeline.named_stages

    def test_name_property(self, config, spark, logger, state, measure):
        """
        Test the 'name' property of the Pipeline.

        This test verifies that the `name` property of the Pipeline correctly reflects the name assigned during
        Pipeline initialization.

        Args:
            config, spark, logger, state, measure: Common fixtures for Pipeline initialization.
        """
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        assert pipeline.name == "test_pipeline"

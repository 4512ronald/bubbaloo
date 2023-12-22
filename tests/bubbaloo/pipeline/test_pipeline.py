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
    def test_pipeline_initialization(self, config, spark, logger, state, measure):
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
        pipeline = Pipeline(conf=config)

        assert pipeline.name is not None
        assert pipeline._logger is not None
        assert pipeline._spark is not None
        assert pipeline._context is not None
        assert isinstance(pipeline._conf, Config)

    def test_name(self, config, spark, logger, state, measure):
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
    def test_adding_stages(self, config, spark, logger, state, measure, batch_extract, batch_transform_1, batch_load):
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

    def test_batch_successful_execution(self, config, spark, logger, state, measure, batch_extract, batch_transform_1,
                                        batch_load, expected_schema):
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
    def test_str_method(self, config, spark, logger, state, measure, batch_extract, batch_transform_1, batch_load):
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
        pipeline = Pipeline(
            pipeline_name="test_pipeline",
            logger=logger,
            spark=spark,
            context=state,
            conf=config,
            measure=measure
        )

        assert pipeline.name == "test_pipeline"

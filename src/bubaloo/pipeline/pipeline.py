from typing import Union, Dict, Callable, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from bubaloo.pipeline import Extract
from bubaloo.pipeline import Load
from bubaloo.pipeline import Transform
from bubaloo.services.pipeline import Session, Config, PipelineState
from bubaloo.services.pipeline.errors import ExecutionError
from bubaloo.services.pipeline.logger import Logger


class Pipeline:

    def __init__(self, **kwargs):
        self._logger: Logger = Logger(self.__class__.__name__)
        self._spark: SparkSession = Session.get_or_create()
        self._context: PipelineState = PipelineState()
        self._params: Dict[str, Any] = kwargs
        self._stages: Dict[str, Union[Transform, Load, Extract]] = {}
        self._conf: Config | None = self._params.get("conf")

    def _add_stages(self):
        stage_types = {
            "extract": Extract,
            "transform": Transform,
            "load": Load
        }
        for stage_name, stage_class in stage_types.items():
            stage_instance = self._params.get(stage_name)
            if stage_instance is not None:
                if isinstance(stage_instance, stage_class):
                    self._stages[stage_name] = stage_instance
                else:
                    error_message = f"Expected an instance of {stage_class.__name__}, got {type(stage_instance)}"
                    self._logger.error(error_message)
                    raise TypeError(error_message)

    def _validate_conf(self):
        if self._conf is None:
            error_message = ("A valid configuration has not been provided. Make sure the 'conf' parameter is passed "
                             "correctly.")
            self._logger.error(error_message)
            raise TypeError(error_message)

    def _initialize_and_execute_extract(self) -> DataFrame:
        extract_stage: Extract = self._stages["extract"]
        extract_stage.initialize(self._conf, self._spark, self._logger, self._context)
        try:
            return extract_stage.execute()
        except Exception as e:
            self._raise_error("Error during extract stage", e)

    def _initialize_and_execute_transform(self) -> Callable[..., None | DataFrame] | None:
        transform_stage: Transform = self._stages.get("transform")
        if transform_stage is None:
            return
        transform_stage.initialize(self._conf, self._spark, self._logger, self._context)
        try:
            return transform_stage.execute()
        except Exception as e:
            self._raise_error("Error during transform stage", e)

    def _initialize_and_execute_load(self, dataframe: DataFrame,
                                     transform: Callable[..., None | DataFrame] | None) -> DataStreamWriter | None:
        load_stage: Load = self._stages["load"]
        load_stage.initialize(self._conf, self._spark, self._logger, self._context)
        try:
            return load_stage.execute(dataframe, transform)
        except Exception as e:
            self._raise_error("Error during load stage", e)

    def _raise_error(self, message, e):
        error_message = message
        self._logger.error(error_message)
        raise ExecutionError(error_message, self.__class__.__name__, e) from e

    def run(self) -> StreamingQuery | None | bool:
        self._add_stages()
        self._validate_conf()

        extract = self._initialize_and_execute_extract()
        transform = self._initialize_and_execute_transform()
        load = self._initialize_and_execute_load(extract, transform)

        try:
            if load is None:
                return
            return load.start().awaitTermination()
        except Exception as e:
            error_message = "Error while starting the data write"
            self._logger.error(error_message)
            raise ExecutionError(error_message, "Stage", e) from e

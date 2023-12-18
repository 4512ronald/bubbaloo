from typing import Union, Dict, Callable, Any, List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from bubbaloo.pipeline.stages.load import Load
from bubbaloo.pipeline.stages.transform import Transform
from bubbaloo.pipeline.stages.extract import Extract
from bubbaloo.services.pipeline.config import Config
from bubbaloo.services.pipeline.measure import Measure
from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.errors.errors import ExecutionError
from bubbaloo.utils.functions.pipeline_stages_helper import validate_params, raise_error
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


class Pipeline:

    def __init__(self, **kwargs):
        self._params: Dict[str, Any] = validate_params(kwargs)
        self._name: str = self._params.get("name")
        self._logger: ILogger = self._params.get("logger")
        self._spark: SparkSession = self._params.get("spark")
        self._context: PipelineState = self._params.get("context")
        self._conf: Config = self._params.get("conf")
        self._measure: Measure = self._params.get("measure")
        self._is_streaming: bool = False
        self._named_stages: Dict[str, Union[DataFrame, Callable[..., None | DataFrame], DataStreamWriter, None]] = {}
        self._pipeline: DataStreamWriter | None = None
        self._stages: Dict[str, Union[List[Tuple[str, Transform]], Tuple[str, Load], Tuple[str, Extract], None]] = {
            "extract": None,
            "transform": [],
            "load": None
        }

    def __str__(self):
        return f"name: {self._name}, stages: {self._named_stages}"

    @property
    def is_streaming(self) -> bool:
        return self._is_streaming

    @property
    def named_stages(self) -> Dict[str, Union[DataFrame, Callable[..., None | DataFrame], DataStreamWriter, None]]:
        return self._named_stages

    @property
    def name(self) -> str:
        return self._name

    def stages(self, stages: List[Tuple[str, Union[Transform, Load, Extract]]]) -> "Pipeline":
        for stage_name, stage_instance in stages:

            if isinstance(stage_instance, Extract):
                self._stages["extract"] = stage_name, stage_instance
            elif isinstance(stage_instance, Transform):
                self._stages["transform"].append((stage_name, stage_instance))
            elif isinstance(stage_instance, Load):
                self._stages["load"] = stage_name, stage_instance
            else:
                error_message = f"Expected an instance of Extract, Transform or Load, got {type(stage_instance)}"
                self._logger.error(error_message)
                raise TypeError(error_message)

        self._prepare_pipeline()

        return self

    def _initialize_and_execute_extract(self) -> DataFrame:
        name, extract_stage = self._stages["extract"]
        extract_stage.initialize(self._conf, self._spark, self._logger, self._context, self._measure)
        try:
            dataframe = extract_stage.execute()
            self._named_stages[name] = dataframe
            self._is_streaming = dataframe.isStreaming
            return dataframe
        except Exception as e:
            raise_error(self._logger, self.__class__.__name__, "Error during extract stage", e)

    def _initialize_and_execute_transform(
            self,
            dataframe: DataFrame = None
    ) -> Callable[..., None | DataFrame] | None:

        transform_stages: List[Tuple[str, Transform]] = self._stages.get("transform")
        transforms = dataframe

        if not transform_stages:
            return

        for name, stage in transform_stages:
            stage.initialize(self._conf, self._spark, self._logger, self._context, self._measure)

            try:
                result = stage.execute(transforms)
                self._named_stages[name] = result
                if callable(result):
                    return result
                elif isinstance(result, DataFrame):
                    transforms = result
                else:
                    raise ValueError(f"Expected return a DataFrame or Callable, got {type(result)}")
            except Exception as e:
                raise_error(self._logger, self.__class__.__name__, "Error during transform stage", e)

        return transforms

    def _initialize_and_execute_load(
            self,
            dataframe: DataFrame,
            transform: Callable[..., None | DataFrame] | None
    ) -> DataStreamWriter | None:

        name, load_stage = self._stages["load"]
        load_stage.initialize(self._conf, self._spark, self._logger, self._context, self._measure)
        try:
            result = load_stage.execute(dataframe, transform)
            self._named_stages[name] = result
            return result
        except Exception as e:
            raise_error(self._logger, self.__class__.__name__, "Error during load stage", e)

    def _prepare_pipeline(self) -> None:
        extract = self._initialize_and_execute_extract()
        transform = self._initialize_and_execute_transform(extract)
        load = self._initialize_and_execute_load(extract, transform)

        self._pipeline = load

    def _reset_pipeline(self) -> None:
        self._pipeline = None
        self._stages = {
            "extract": None,
            "transform": [],
            "load": None
        }

    def execute(self) -> StreamingQuery | None | bool:
        load = self._pipeline
        self._reset_pipeline()
        try:
            if not self._is_streaming:
                return
            return load.start().awaitTermination()
        except Exception as e:
            error_message = "Error while starting the data write"
            self._logger.error(error_message)
            raise ExecutionError(error_message, "Stage", e) from e

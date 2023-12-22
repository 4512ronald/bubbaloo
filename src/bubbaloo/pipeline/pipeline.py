from typing import Union, Dict, Callable, Any, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from bubbaloo.errors.errors import ExecutionError
from bubbaloo.services.pipeline import measure
from bubbaloo.utils.functions.pipeline_stage_helper import validate_params, raise_error
from bubbaloo.utils.interfaces.pipeline_logger import ILogger
from bubbaloo.pipeline.stages.load import Load
from bubbaloo.pipeline.stages.transform import Transform
from bubbaloo.pipeline.stages.extract import Extract
from bubbaloo.services.pipeline.config import Config
from bubbaloo.services.pipeline.state import PipelineState


OutputStageType = DataFrame | Callable[..., None | DataFrame] | DataStreamWriter | None
StageType = Tuple[str, Transform | Load | Extract]
InputStageType = List[StageType] | StageType | None


class Pipeline:
    """
    Manages the orchestration and execution of data processing stages in a Spark application.

    This class provides a structured way to define and execute a sequence of data processing
    stages, typically including extract, transform, and load (ETL) operations. It supports both
    batch and streaming data processing modes. The Pipeline class allows for flexible
    configuration and execution of these stages, with built-in support for error handling and
    logging.

    Attributes:
        _params (Dict[str, Any]): Configuration parameters for the pipeline, validated and
                                  processed from the provided keyword arguments.
        _name (str): The name of the pipeline, used for identification and logging purposes.
        _logger (ILogger): An instance of a logger, adhering to the ILogger interface, used
                           for logging events and errors throughout the pipeline's execution.
        _spark (SparkSession): A SparkSession instance used for executing Spark-related
                               operations within the pipeline stages.
        _context (PipelineState): The context or state of the pipeline, encapsulating any
                                  necessary state information required across different stages.
        _conf (Config): Configuration settings specific to the pipeline, encapsulating various
                        operational parameters and settings.
        _is_streaming (bool): A boolean flag indicating whether the pipeline is configured
                              for streaming data processing.
        _named_stages (Dict[str, Union[DataFrame, Callable[..., None | DataFrame], DataStreamWriter, None]]):
                        A dictionary holding references to named stages (like extract, transform,
                        and load) along with their respective outputs or callable references.
        _pipeline (DataStreamWriter | None): A reference to a DataStreamWriter, if applicable,
                                             used in the context of Spark streaming operations.
        _stages (Dict[str, Union[List[Tuple[str, Transform]], Tuple[str, Load], Tuple[str, Extract], None]]):
                 A dictionary mapping stage types (extract, transform, load) to their respective
                 stage instances or configurations.

    Methods:
        __init__(**kwargs): Constructor for initializing the Pipeline with specified parameters.
        __str__(): Provides a string representation of the Pipeline instance.
        is_streaming (property): Indicates whether the pipeline is configured for streaming data.
        named_stages (property): Retrieves the named stages of the pipeline.
        name (property): Retrieves the name of the pipeline.
        stages(stages): Method for setting the stages of the pipeline.
        _initialize_and_execute_extract(): Initializes and executes the extract stage.
        _initialize_and_execute_transform(dataframe): Initializes and executes the transform stage.
        _initialize_and_execute_load(dataframe, transform): Initializes and executes the load stage.
        _prepare_pipeline(): Prepares the pipeline for execution.
        _reset_pipeline(): Resets the pipeline to its initial state.
        execute(): Executes the pipeline and manages streaming query execution.
    """

    def __init__(self, **kwargs):
        """
        Initializes the Pipeline with specified parameters.

        Args:
            **kwargs: Arbitrary keyword arguments for the pipeline's configuration.
                Expected keys include 'pipeline_name', 'logger', 'spark', 'context',
                'conf', and 'measure'.
        """

        self._params: Dict[str, Any] = validate_params(kwargs)
        self._name: str = self._params.get("pipeline_name")
        self._logger: ILogger = self._params.get("logger")
        self._spark: SparkSession = self._params.get("spark")
        self._context: PipelineState = self._params.get("context")
        self._conf: Config = self._params.get("conf")
        self._is_streaming: bool = False
        self._named_stages: Dict[str, OutputStageType] = {}
        self._pipeline: DataStreamWriter | None = None
        self._stages: Dict[str, InputStageType] = {
            "extract": None,
            "transform": [],
            "load": None
        }

    def __str__(self):
        """
        Returns a string representation of the Pipeline.

        Provides a concise summary of the pipeline, including its name and the stages
        it contains.

        Returns:
            str: A string describing the pipeline.
        """

        return f"name: {self._name}, stages: {self._named_stages}"

    @property
    def is_streaming(self) -> bool:
        """
        Indicates whether the pipeline is configured for streaming data.

        Returns:
            bool: True if the pipeline is set up for streaming data, False otherwise.
        """
        return self._is_streaming

    @property
    def named_stages(self) -> Dict[str, OutputStageType]:
        """
        Gets the named stages of the pipeline.

        Returns a dictionary of the stages in the pipeline, keyed by their names.
        Each value in the dictionary can be a DataFrame, a Callable, a DataStreamWriter, or None.

        Returns:
            Dict[str, Union[DataFrame, Callable[..., None | DataFrame], DataStreamWriter, None]]:
                The named stages of the pipeline.
        """

        return self._named_stages

    @property
    def name(self) -> str:
        """
        Gets the name of the pipeline.

        Returns:
            str: The name of the pipeline.
        """
        return self._name

    def stages(self, stages: List[StageType]) -> "Pipeline":
        """
        Sets the stages for the pipeline.

        Args:
            stages (List[Tuple[str, Union[Transform, Load, Extract]]]):
                A list of tuples, each containing the name of the stage and an instance
                of either Transform, Load, or Extract.

        Returns:
            Pipeline: The pipeline instance with updated stages.
        """
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

        return self

    def _initialize_and_execute_extract(self) -> DataFrame:
        """
        Initializes and executes the extract stage of the pipeline.

        This method prepares and runs the extraction process, using the configuration
        and parameters specified in the pipeline.

        Returns:
            DataFrame: The DataFrame resulting from the extract stage.
        """
        name, extract_stage = self._stages["extract"]
        extract_stage.initialize(self._conf, self._spark, self._logger, self._context)
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
    ) -> Callable[..., None] | DataFrame | None:
        """
        Initializes and executes the transform stages of the pipeline.

        Applies the transformation stages to the provided DataFrame. Each transform
        stage is initialized and executed in sequence.

        Args:
            dataframe (DataFrame, optional): The DataFrame to apply transformations on.
                Defaults to None.

        Returns:
            Union[Callable[..., None] | DataFrame, None]: The result of the transform
                stages, which can be a modified DataFrame or a callable function.
        """
        transform_stages: List[StageType] = self._stages.get("transform")
        transformed_df = dataframe

        if not transform_stages:
            return

        for name, stage in transform_stages:
            stage.initialize(self._conf, self._spark, self._logger, self._context)

            try:
                result = stage.execute(transformed_df)
                self._named_stages[name] = result
                if callable(result):
                    return result
                elif isinstance(result, DataFrame):
                    transformed_df = result
                else:
                    raise ValueError(f"Expected return a DataFrame or Callable, got {type(result)}")
            except Exception as e:
                raise_error(self._logger, self.__class__.__name__, "Error during transform stage", e)

        return transformed_df

    def _initialize_and_execute_load(
            self,
            dataframe: DataFrame,
            transform: Callable[..., None] | DataFrame | None
    ) -> DataStreamWriter | None:

        """
        Initializes and executes the load stage of the pipeline.

        This method prepares and runs the loading process, taking into account any
        transformations applied.

        Args:
            dataframe (DataFrame): The DataFrame to be loaded.
            transform (Callable[..., None] | DataFrame | None): An optional
                transform to be applied during the load process.

        Returns:
            Union[DataStreamWriter, None]: The DataStreamWriter resulting from the
                load stage, if applicable.
        """
        if isinstance(transform, DataFrame):
            dataframe = transform
            transform = None

        name, load_stage = self._stages["load"]

        load_stage.initialize(self._conf, self._spark, self._logger, self._context)

        try:
            result = load_stage.execute(dataframe, transform)
            self._named_stages[name] = result
            return result
        except Exception as e:
            raise_error(self._logger, self.__class__.__name__, "Error during load stage", e)

    def _prepare_pipeline(self) -> DataStreamWriter | None:
        """
        Prepares the pipeline for execution.

        This method initializes and sets up the extract, transform, and load stages
        in preparation for the pipeline execution.
        """
        extract = self._initialize_and_execute_extract()
        transform = self._initialize_and_execute_transform(extract)
        load = self._initialize_and_execute_load(extract, transform)

        return load

    def _reset_pipeline(self) -> None:
        """
        Resets the pipeline to its initial state.

        Clears the current configuration of the pipeline, including the stages and
        the DataStreamWriter if set.
        """
        self._pipeline = None
        self._stages = {
            "extract": None,
            "transform": [],
            "load": None
        }

    @measure.time
    def execute(self) -> StreamingQuery | bool | None:
        """
        Executes the pipeline and manages the streaming query execution.

        Runs the entire pipeline process, handling streaming data if configured.
        Resets the pipeline configuration upon completion.

        Returns:
            Union[StreamingQuery, None, bool]: The result of the pipeline execution,
                which can be a StreamingQuery, None, or a boolean indicating success or failure.
        """
        pipeline = self._prepare_pipeline()
        self._reset_pipeline()

        if not self._is_streaming:
            return

        try:
            return pipeline.start().awaitTermination()
        except Exception as e:
            error_message = "Error while starting the data write"
            self._logger.error(error_message)
            raise ExecutionError(error_message, "Stage", e) from e

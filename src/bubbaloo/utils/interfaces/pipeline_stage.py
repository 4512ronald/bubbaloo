from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from bubbaloo.services.pipeline.config import Config
from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


class StageAbstract(ABC):
    """
    Abstract base class for defining stages in a data processing pipeline.

    This class provides a common interface for various stages of a data pipeline,
    facilitating initialization and execution of each stage. It is designed to be
    subclassed with specific implementations for different tasks in the pipeline.

    Attributes:
        conf (Config): Configuration settings for the pipeline stage.
        spark (SparkSession): Spark session to be used in the stage.
        logger (ILogger): Logger for logging messages.
        context (PipelineState): State of the pipeline, carrying contextual information.
    """

    def __init__(self):
        """
        Initializes the StageAbstract with default values.
        """
        self.conf: Config | None = None
        self.spark: SparkSession | None = None
        self.logger: ILogger | None = None
        self.context: PipelineState | None = None

    def initialize(
            self,
            conf: Config,
            spark: SparkSession,
            logger: ILogger,
            context: PipelineState,
    ) -> None:
        """
        Initializes the stage with the necessary components.

        This method sets up the stage with configuration, Spark session, logger,
        pipeline state, and measurement tools.

        Args:
            conf (Config): Configuration settings for the stage.
            spark (SparkSession): Spark session for the stage.
            logger (ILogger): Logger for the stage.
            context (PipelineState): Contextual state of the pipeline.
        """
        self.conf = conf
        self.spark = spark
        self.logger = logger
        self.context = context

    @abstractmethod
    def execute(self, *args, **kwargs):
        """
        Abstract method to execute the stage.

        This method should be implemented by each subclass to define the specific
        operations to be performed in the stage. The method can accept any number
        of arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

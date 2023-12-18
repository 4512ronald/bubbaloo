from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from bubbaloo.services.pipeline.config import Config
from bubbaloo.services.pipeline.state import PipelineState
from bubbaloo.services.pipeline.measure import Measure
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


class StageAbstract(ABC):

    def __init__(self):
        self.conf = None
        self.spark = None
        self.logger = None
        self.context = None
        self.measure = None

    def initialize(self, conf: Config, spark: SparkSession, logger: ILogger, context: PipelineState, measure: Measure):
        self.conf = conf
        self.spark = spark
        self.logger = logger
        self.context = context
        self.measure = measure

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass

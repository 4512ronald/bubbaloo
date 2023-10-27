from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

from bubaloo.shared.interfaces.stage import Stage
from bubaloo.tools.pipeline.config import Config
from bubaloo.tools.pipeline.logger import Logger
from bubaloo.tools.pipeline.state import PipelineState


class Extract(Stage, ABC):

    @abstractmethod
    def execute(self) -> DataFrame:
        pass

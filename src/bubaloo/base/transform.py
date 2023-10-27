from abc import ABC, abstractmethod
from typing import Callable, Any
from pyspark.sql import DataFrame, SparkSession

from bubaloo.shared.interfaces.stage import Stage
from bubaloo.tools.pipeline.config import Config
from bubaloo.tools.pipeline.logger import Logger
from bubaloo.tools.pipeline.state import PipelineState


class Transform(Stage, ABC):

    @abstractmethod
    def execute(self) -> Callable[..., None | DataFrame]:
        pass



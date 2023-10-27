from abc import ABC, abstractmethod
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from bubaloo.shared.interfaces.stage import Stage
from bubaloo.tools.pipeline.config import Config
from bubaloo.tools.pipeline.logger import Logger
from bubaloo.tools.pipeline.state import PipelineState


class Load(Stage, ABC):

    @abstractmethod
    def execute(self, dataframe: DataFrame, transform: Callable[..., None | DataFrame] | None) -> DataStreamWriter | None:
        pass


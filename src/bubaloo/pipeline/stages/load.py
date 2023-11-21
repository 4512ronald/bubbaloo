from abc import ABC, abstractmethod
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from bubaloo.utils.interfaces.pipeline_stages_stage import Stage


class Load(Stage, ABC):

    @abstractmethod
    def execute(self, dataframe: DataFrame, transform: Callable[..., None | DataFrame] | None) -> DataStreamWriter | None:
        pass


from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Load(StageAbstract, ABC):

    @abstractmethod
    def execute(
            self,
            dataframe: DataFrame,
            transform: Callable[..., None | DataFrame] | None
    ) -> DataStreamWriter | None:
        pass

from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql import DataFrame

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Transform(StageAbstract, ABC):

    @abstractmethod
    def execute(self, *args: DataFrame | None) -> Callable[..., None] | DataFrame:
        pass

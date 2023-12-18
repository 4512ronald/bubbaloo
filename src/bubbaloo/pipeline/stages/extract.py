from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Extract(StageAbstract, ABC):

    @abstractmethod
    def execute(self) -> DataFrame:
        pass

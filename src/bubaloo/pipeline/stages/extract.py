from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from bubaloo.utils.interfaces.pipeline_stages_stage import Stage


class Extract(Stage, ABC):

    @abstractmethod
    def execute(self) -> DataFrame:
        pass

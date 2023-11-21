from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql import DataFrame

from bubaloo.utils.interfaces.pipeline_stages_stage import Stage


class Transform(Stage, ABC):

    @abstractmethod
    def execute(self) -> Callable[..., None | DataFrame]:
        pass



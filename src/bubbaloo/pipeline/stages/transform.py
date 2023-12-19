from abc import ABC, abstractmethod
from typing import Callable
from pyspark.sql import DataFrame

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Transform(StageAbstract, ABC):
    """
    Abstract class for the transformation stage in a data pipeline.

    This class defines the interface for transformation operations within a data pipeline.
    Subclasses should implement the `execute` method to define specific logic for
    transforming data. The transformation process typically involves manipulating,
    cleaning, or enriching the data.

    This class extends StageAbstract, adhering to its interface for pipeline stages.
    """

    @abstractmethod
    def execute(self, *args: DataFrame | None) -> Callable[..., None] | DataFrame:
        """
        Abstract method to execute the data transformation process.

        This method should be implemented by subclasses to specify the transformation logic.
        The implementation can accept multiple DataFrames and should return either a modified
        DataFrame or a Callable that performs the transformation.

        Args:
            *args (DataFrame | None): One or more DataFrames to be transformed.

        Returns:
            Callable[..., None] | DataFrame: A transformed DataFrame or a Callable that
                performs the transformation when invoked.
        """
        pass

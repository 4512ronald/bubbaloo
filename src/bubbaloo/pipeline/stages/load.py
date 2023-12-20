from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Load(StageAbstract, ABC):
    """
    Abstract class for the loading stage in a data pipeline.

    This class defines the interface for load operations within a data pipeline.
    Subclasses should implement the `execute` method to define specific logic
    for loading data into a destination, such as a database or a data lake. The
    loading process may involve transformations on the data.

    This class extends StageAbstract, adhering to its interface for pipeline stages.
    """

    @abstractmethod
    def execute(
            self,
            dataframe: DataFrame,
            transform: Callable[..., None] | DataFrame | None
    ) -> DataStreamWriter | None:
        """
        Abstract method to execute the data loading process.

        This method should be implemented by subclasses to specify the loading logic.
        The implementation may include transforming the data before loading. The
        transformation is provided as an optional callable.

        Args:
            dataframe (DataFrame): The DataFrame to be loaded.
            transform (Callable[..., None | DataFrame] | None): An optional callable
                that takes a DataFrame and returns a transformed DataFrame or None.

        Returns:
            DataStreamWriter | None: A DataStreamWriter object if applicable, or None.
        """
        pass

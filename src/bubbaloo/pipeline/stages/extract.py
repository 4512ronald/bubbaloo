from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from bubbaloo.utils.interfaces.pipeline_stage import StageAbstract


class Extract(StageAbstract, ABC):
    """
    Abstract class for the extraction stage in a data pipeline.

    This class defines the interface for extraction operations within a data pipeline.
    Subclasses should implement the `execute` method to define specific data extraction
    logic. The data extraction typically involves retrieving data from various sources
    and presenting it as a DataFrame for further processing in the pipeline.

    This class extends StageAbstract, adhering to its interface for pipeline stages.
    """

    @abstractmethod
    def execute(self) -> DataFrame:
        """
        Abstract method to execute the data extraction process.

        This method should be implemented by subclasses to specify the extraction logic.
        The implementation should return a DataFrame containing the extracted data.

        Returns:
            DataFrame: The DataFrame containing the extracted data.
        """
        pass

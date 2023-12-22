from datetime import datetime
from typing import Dict, List


class PipelineState:
    """
    Singleton class used for tracking and managing state throughout the execution of a pipeline.

    This class stores various state information such as errors, duration, counts, batch ID,
    and entity information. It ensures that only one instance of this state is created and
    used throughout the pipeline process. The class provides methods to access and modify
    the state as well as to retrieve a summary of the current state.
    """

    _instance = None
    _resume = []

    def __new__(cls, *args, **kwargs):
        """
        Creates a new instance of the class if one doesn't already exist.

        Returns the existing instance if already created, ensuring the singleton pattern.

        Returns:
            An instance of PipelineState.
        """
        if cls._instance is None:
            cls._instance = super(PipelineState, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initializes the PipelineState instance.

        Sets up initial state values if the instance is being initialized for the first time.
        """
        if not hasattr(self, '_initialized'):
            self._errors: Dict[str, str] = {
                "DataProcessing": "No errors",
                "PipelineExecution": "No errors"
            }
            self._duration: int = 0
            self._count: Dict[str, int] = {}
            self._batch_id: int = 0
            self._entity: str = ""
            self._resume: Dict[str, str] | None = None
            self._initialized = True

    @property
    def errors(self) -> Dict[str, str]:
        """
        Returns the current errors in the pipeline state.

        Returns:
            Dict[str, str | List[Dict[str, list]]]: The current error states.
        """
        return self._errors

    @property
    def duration(self) -> int:
        """
        Returns the total duration of the pipeline processing.

        Returns:
            int: The duration in some unit of time.
        """
        return self._duration

    @property
    def count(self) -> Dict[str, int]:
        """
        Returns a dictionary of counts relevant to the pipeline state.

        Returns:
            Dict[str, int]: The counts associated with various aspects of the pipeline.
        """
        return self._count

    @property
    def batch_id(self) -> int:
        """
        Returns the current batch ID in the pipeline state.

        Returns:
            int: The batch ID.
        """
        return self._batch_id

    @property
    def entity(self) -> str:
        """
        Returns the entity associated with the current pipeline state.

        Returns:
            str: The entity name or identifier.
        """
        return self._entity

    @errors.setter
    def errors(self, error: Dict[str, str]) -> None:
        """
        Sets the errors attribute in the pipeline state.

        Args:
            error (Dict[str, str]): A dictionary representing the error states to set.
        """
        self._errors = error

    @duration.setter
    def duration(self, duration: int) -> None:
        """
        Sets the duration attribute in the pipeline state.

        Args:
            duration (int): The duration to set in some unit of time.
        """
        self._duration = duration

    @count.setter
    def count(self, count: Dict[str, int]) -> None:
        """
        Sets the count dictionary in the pipeline state.

        Args:
            count (Dict[str, int]): The count dictionary to set.
        """
        self._count = count

    @batch_id.setter
    def batch_id(self, batch_id: int) -> None:
        """
        Sets the batch ID in the pipeline state.

        Args:
            batch_id (int): The batch ID to set.
        """
        self._batch_id = batch_id

    @entity.setter
    def entity(self, entity: str) -> None:
        """
        Sets the entity associated with the pipeline state.

        Args:
            entity (str): The entity name or identifier to set.
        """
        self._entity = entity

    @classmethod
    def _add_entry(cls) -> None:
        """
        Generates and returns a summary of the current pipeline state.

        Returns:
            Dict[str, str]: A summary of the pipeline state including entity, operation metrics,
                            errors, batch ID, duration, and timestamp.
        """
        cls._resume.append({
            "entity": cls._instance.entity,
            "operationMetrics": cls._instance.count,
            "errors": cls._instance.errors,
            "batchId": cls._instance.batch_id,
            "duration": cls._instance.duration,
            "timestamp": datetime.now().isoformat(sep=" ", timespec="seconds"),
        })

    @classmethod
    def reset(cls) -> None:
        """
        Resets the PipelineState singleton instance.

        This method allows for the reinitialization of the PipelineState for new pipeline executions.
        """
        cls._add_entry()

        cls._instance._errors = {
            "DataProcessing": "No errors",
            "PipelineExecution": "No errors"
        }
        cls._instance._duration = 0
        cls._instance._count = {}
        cls._instance._batch_id = 0
        cls._instance._entity = ""

    @classmethod
    def resume(cls) -> List[Dict[str, str]]:
        """
        Returns the current pipeline state summary.

        Returns:
            List[Dict[str, str]]: The current pipeline state summary.
        """

        resume = cls._resume
        cls._resume = []
        return resume

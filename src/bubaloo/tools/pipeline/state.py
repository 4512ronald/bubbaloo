from datetime import datetime
from typing import Dict, Optional


class PipelineState:
    """A class representing the state of a pipeline stage using Singleton pattern."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PipelineState, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize instance variables."""
        self._errors: Dict[str, str] = {"dataProcessing": "No errors", "pipelineStage": "No errors"}
        self._duration: int = 0
        self._count: Dict[str, int] = {}
        self._batch_id: int = 0
        self._entity: str = ""
        self._resume: Optional[Dict[str, str]] = None

    @property
    def errors(self) -> Dict[str, str]:
        return self._errors

    @property
    def duration(self) -> int:
        return self._duration

    @property
    def count(self) -> Dict[str, int]:
        return self._count

    @property
    def batch_id(self) -> int:
        return self._batch_id

    @property
    def entity(self) -> str:
        return self._entity

    @errors.setter
    def errors(self, error: Dict[str, str]) -> None:
        self._errors = error

    @duration.setter
    def duration(self, duration: int) -> None:
        self._duration = duration

    @count.setter
    def count(self, count: Dict[str, int]) -> None:
        self._count = count

    @batch_id.setter
    def batch_id(self, batch_id: int) -> None:
        self._batch_id = batch_id

    @entity.setter
    def entity(self, entity: str) -> None:
        self._entity = entity

    def get_resume(self) -> Dict[str, str]:
        self._resume = {
            "entity": self._entity,
            "operationMetrics": self._count,
            "errors": self._errors,
            "batchId": self._batch_id,
            "duration": self._duration,
            "timestamp": datetime.now().isoformat(sep=" ", timespec="seconds"),
        }
        return self._resume

    def reset(self) -> None:
        self._initialize()

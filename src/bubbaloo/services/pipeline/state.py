from datetime import datetime
from typing import Dict, List


class PipelineState:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PipelineState, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._errors: Dict[str, str | List[Dict[str, list]]] = {
                "dataProcessing": "No errors",
                "pipelineStage": "No errors"
            }
            self._duration: int = 0
            self._count: Dict[str, int] = {}
            self._batch_id: int = 0
            self._entity: str = ""
            self._resume: Dict[str, str] | None = None
            self._initialized = True

    @property
    def errors(self) -> Dict[str, str | List[Dict[str, list]]]:
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

    @classmethod
    def reset(cls) -> None:
        cls._instance = None

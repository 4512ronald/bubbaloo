from abc import ABC, abstractmethod


class IValidation(ABC):

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        pass

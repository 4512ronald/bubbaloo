from abc import ABC, abstractmethod


class IStorageManager(ABC):

    @abstractmethod
    def list(self, *args, **kwargs):
        pass

    @abstractmethod
    def copy(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        pass

    @abstractmethod
    def move(self, *args, **kwargs):
        pass

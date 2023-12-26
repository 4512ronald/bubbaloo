from abc import ABC, abstractmethod


class IStorageManager(ABC):
    """
    Abstract base class defining an interface for storage management operations.

    This class serves as a template for implementing storage management functionalities
    such as listing, copying, deleting, and moving items within a storage system.
    It is intended to be subclassed with specific implementations for different storage
    backends or strategies.

    The methods to be implemented are `list`, `copy`, `delete`, and `move`, each
    potentially accepting a variety of arguments and keyword arguments to accommodate
    different storage operations.
    """

    @abstractmethod
    def list(self, *args, **kwargs):
        """
        Abstract method to list items in the storage.

        Implementing classes should define the specific behavior for listing items,
        potentially based on provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def copy(self, *args, **kwargs):
        """
        Abstract method to copy items within the storage.

        Implementing classes should define the specific behavior for copying items,
        potentially based on provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        """
        Abstract method to delete items from the storage.

        Implementing classes should define the specific behavior for deleting items,
        potentially based on provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def move(self, *args, **kwargs):
        """
        Abstract method to move items within the storage.

        Implementing classes should define the specific behavior for moving items,
        potentially based on provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def filter(self, *args, **kwargs):
        """
        Abstract method to filter items within the storage.

        Implementing classes should define the specific behavior for filtering items,
        potentially based on provided arguments and keyword arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        pass

from abc import ABC, abstractmethod


class ILogger(ABC):
    """
    Abstract base class for a logging interface.

    This class defines a standard interface for logging functionalities. It is
    intended to be implemented by concrete logger classes which provide actual
    logging behavior for various levels such as info, warning, and error.

    The methods defined in this interface are `info`, `warning`, and `error`,
    each expecting a message to log. Concrete implementations should provide
    the specific behavior for these methods.
    """

    @abstractmethod
    def info(self, message: str):
        """
        Abstract method to log an informational message.

        Implementing classes should define how informational messages are logged.

        Args:
            message (str): The informational message to be logged.
        """
        pass

    @abstractmethod
    def warning(self, message: str):
        """
        Abstract method to log a warning message.

        Implementing classes should define how warning messages are logged.

        Args:
            message (str): The warning message to be logged.
        """
        pass

    @abstractmethod
    def error(self, message: str):
        """
        Abstract method to log an error message.

        Implementing classes should define how error messages are logged.

        Args:
            message (str): The error message to be logged.
        """
        pass

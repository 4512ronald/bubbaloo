from abc import ABC, abstractmethod


class IValidation(ABC):
    """
    Abstract base class defining an interface for validation processes.

    This class provides a generic interface for implementing various types
    of validation logic. Subclasses should implement the `execute` method
    to carry out specific validation tasks. This method is designed to be
    flexible, accepting a variable number of arguments and keyword arguments.

    The purpose of this interface is to standardize how validation is
    performed across different components or modules, ensuring a consistent
    approach to data or process validation.
    """

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """
        Abstract method to execute the validation process.

        This method should be implemented by subclasses to define specific
        validation logic. It is designed to be versatile, accommodating a wide
        range of validation scenarios.

        Args:
            *args: Variable length argument list, allowing flexibility in the number
                   of arguments passed to the method.
            **kwargs: Arbitrary keyword arguments, supporting various parameters for
                      validation.
        """
        pass

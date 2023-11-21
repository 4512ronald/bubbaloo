from bubaloo.services.pipeline.session import Session


class Logger:
    """A class for managing logging using Apache Log4j."""

    def __init__(self, name=None):
        """Initialize the LoggerManager.

        Args:
            name (str, optional): The name of the logger. Defaults to the fully qualified class name.

        """
        if name is None:
            name = f"{self.__class__.__module__}.{self.__class__.__name__}"

        self.spark = Session.get_or_create()
        self.logger = self.spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger(name) # noqa

    def info(self, message: str) -> None:
        """Log an informational message.

        Args:
            message (str): The message to log.

        Returns:
            None

        """
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """Log a warning message.

        Args:
            message (str): The message to log.

        Returns:
            None

        """
        self.logger.warn(message)

    def error(self, message: str) -> None:
        """Log an error message.

        Args:
            message (str): The message to log.

        Returns:
            None

        """
        self.logger.error(message)

from bubbaloo.services.pipeline.get_spark import GetSpark
from bubbaloo.utils.interfaces.pipeline_logger import ILogger


class Logger(ILogger):
    """
    Provides logging functionalities by interfacing with Apache log4j through PySpark.

    This class serves as a wrapper for log4j Logger, facilitating logging operations
    within the PySpark environment. It allows for logging messages at different
    levels such as info, warning, and error.

    Attributes:
        spark: An instance of `GetSpark`, used to obtain the Spark context.
        logger: A log4j Logger instance for logging messages.
    """

    def __init__(self, name=None):
        """
        Initializes the Logger instance with a specific logger name.

        If no name is provided, it defaults to "bubbaloo". It sets up the Spark
        context and initializes the log4j Logger.

        Args:
            name: Optional; The name of the logger. Defaults to "bubbaloo" if None.
        """
        if name is None:
            name = "Bubbaloo"

        self.spark = GetSpark()
        self.logger = self.spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger(name) # noqa

    def info(self, message: str) -> None:
        """
        Logs a message at the info level.

        Args:
            message: The message to be logged at the info level.
        """
        self.logger.info(message)

    def warning(self, message: str) -> None:
        """
        Logs a message at the warning level.

        Args:
            message: The message to be logged at the warning level.
        """
        self.logger.warn(message)

    def error(self, message: str) -> None:
        """
        Logs a message at the error level.

        Args:
            message: The message to be logged at the error level.
        """
        self.logger.error(message)

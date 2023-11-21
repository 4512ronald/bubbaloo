from pyspark.sql import SparkSession


class Session:
    """A class for managing the SparkSession instance."""

    _spark_session = None

    @classmethod
    def start(cls) -> None:
        if cls._spark_session is None:
            cls._spark_session = (
                SparkSession
                .builder
                .getOrCreate()
            )

    @classmethod
    def get_or_create(cls) -> SparkSession:
        """Retrieve the SparkSession instance.

        Returns:
            SparkSession: The SparkSession instance.

        """
        if cls._spark_session is None:
            cls.start()
        return cls._spark_session

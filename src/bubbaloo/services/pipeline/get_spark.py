from pyspark import SparkConf
from pyspark.sql import SparkSession


class GetSpark:
    """
    Singleton class for creating and retrieving a SparkSession instance.

    This class ensures that only one instance of SparkSession is created and
    reused throughout the application. If a Spark configuration is not provided,
    it uses the default SparkConf. This class is intended to simplify the management
    of SparkSession instances in applications.

    Attributes:
        _instance: The single instance of SparkSession.
    """

    _instance = None

    def __new__(cls, conf=None, *args, **kwargs):
        """
        Creates a new SparkSession instance if one doesn't already exist.

        If a SparkSession instance does not exist, it creates a new one with the provided
        SparkConf or the default configuration. If an instance already exists, it returns
        the existing instance, thus maintaining the singleton pattern.

        Args:
            conf (SparkConf, optional): The Spark configuration to use. If None,
                                        defaults to a new SparkConf instance.

        Returns:
            SparkSession: The singleton SparkSession instance.
        """
        if cls._instance is None:
            if conf is None:
                conf = SparkConf()
            cls._instance = SparkSession.builder.config(conf=conf).getOrCreate()
        return cls._instance

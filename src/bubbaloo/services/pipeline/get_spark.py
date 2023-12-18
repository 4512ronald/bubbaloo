from pyspark import SparkConf
from pyspark.sql import SparkSession


class GetSpark:
    _instance = None

    def __new__(cls, conf=None, *args, **kwargs):
        if cls._instance is None:
            if conf is None:
                conf = SparkConf()
            cls._instance = SparkSession.builder.config(conf=conf).getOrCreate()
        return cls._instance

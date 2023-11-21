from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from bubaloo.services.pipeline import Config, Logger, PipelineState


class Stage(ABC):

    def __init__(self):
        self.conf = None
        self.spark = None
        self.logger = None
        self.context = None

    def initialize(self, conf: Config, spark: SparkSession, logger: Logger, context: PipelineState):
        """Método de inicialización para configurar atributos comunes."""
        self.conf = conf
        self.spark = spark
        self.logger = logger
        self.context = context

    @abstractmethod
    def execute(self, *args, **kwargs):
        """Método abstracto para ser implementado por subclases."""
        pass

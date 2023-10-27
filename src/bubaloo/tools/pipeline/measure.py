import time

from bubaloo.tools.pipeline.config import Config
from bubaloo.tools.pipeline.session import Session
from bubaloo.tools.pipeline.state import PipelineState


class Measure:
    """A class for measuring and recording stage metrics."""

    def __init__(self):
        self.spark = Session.get_or_create()
        self.conf = Config.get()
        self.context = PipelineState()

    def time(self, func):
        """Decorator for measuring the execution time of a function.

        Args:
            func (callable): The function to measure.

        Returns:
            callable: The wrapped function.

        """
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            self.context.duration = end_time - start_time
            return result

        return wrapper

    def id(self, func):
        """Decorator for counting the number of records processed by a function.

        Args:
            func (callable): The function to count records.

        Returns:
            callable: The wrapped function.

        """
        def wrapper(*args, **kwargs):
            self.context.batch_id = args[1]
            result = func(*args, **kwargs)
            return result

        return wrapper

    def entity(self, func):
        """Decorator for setting the entity being processed by a function.

        Args:
            func (callable): The function to set the entity.

        Returns:
            callable: The wrapped function.

        """
        def wrapper(*args, **kwargs):
            self.context.entity = args[1]
            return func(*args, **kwargs)

        return wrapper


measure = Measure()

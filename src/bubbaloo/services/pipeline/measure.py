import time

from bubbaloo.services.pipeline.state import PipelineState


class Measure:
    def __init__(self):
        self.context = PipelineState()

    def time(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            self.context.duration = end_time - start_time
            return result

        return wrapper

    def id(self, func):
        def wrapper(*args, **kwargs):
            self.context.batch_id = args[1]
            result = func(*args, **kwargs)
            return result

        return wrapper

    def entity(self, func):
        def wrapper(*args, **kwargs):
            self.context.entity = args[1]
            return func(*args, **kwargs)

        return wrapper

from bubbaloo.services.local.logger import Logger
from bubbaloo.services.pipeline.get_spark import GetSpark
from bubbaloo.services.pipeline.state import PipelineState

LOGGER = Logger()
SPARK = GetSpark()
CONTEXT = PipelineState()
PIPELINE_NAME = "Default Pipeline"

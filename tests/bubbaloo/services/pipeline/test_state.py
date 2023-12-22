import pytest
from bubbaloo.services.pipeline.state import PipelineState


@pytest.fixture
def pipeline_state():
    state = PipelineState()
    state.errors = {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
    state.duration = 10
    state.count = {"count1": 1, "count2": 2}
    state.batch_id = 1
    state.entity = "Entity1"
    return state


class TestPipelineState:
    def test_singleton(self, pipeline_state):
        state2 = PipelineState()
        assert pipeline_state is state2

    def test_initialization(self, pipeline_state):
        assert pipeline_state.errors == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert pipeline_state.duration == 10
        assert pipeline_state.count == {"count1": 1, "count2": 2}
        assert pipeline_state.batch_id == 1
        assert pipeline_state.entity == "Entity1"

    def test_setters_getters(self, pipeline_state):
        pipeline_state.errors = {"DataProcessing": "Error1", "PipelineExecution": "Error2"}
        pipeline_state.duration = 20
        pipeline_state.count = {"count1": 3, "count2": 4}
        pipeline_state.batch_id = 2
        pipeline_state.entity = "Entity2"

        assert pipeline_state.errors == {"DataProcessing": "Error1", "PipelineExecution": "Error2"}
        assert pipeline_state.duration == 20
        assert pipeline_state.count == {"count1": 3, "count2": 4}
        assert pipeline_state.batch_id == 2
        assert pipeline_state.entity == "Entity2"

    def test_reset(self, pipeline_state):
        pipeline_state.reset()

        assert pipeline_state.errors == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert pipeline_state.duration == 0
        assert pipeline_state.count == {}
        assert pipeline_state.batch_id == 0
        assert pipeline_state.entity == ""

    def test_resume(self, pipeline_state):
        resume = pipeline_state.resume()

        assert len(resume) == 1
        assert resume[0]["entity"] == "Entity1"
        assert resume[0]["operationMetrics"] == {"count1": 1, "count2": 2}
        assert resume[0]["errors"] == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert resume[0]["batchId"] == 1
        assert resume[0]["duration"] == 10
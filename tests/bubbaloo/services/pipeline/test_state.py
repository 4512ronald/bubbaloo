import pytest
from bubbaloo.services.pipeline.state import PipelineState


@pytest.fixture
def pipeline_state():
    """
    A pytest fixture that creates and returns an instance of PipelineState.

    This fixture initializes a PipelineState object with pre-defined errors, duration, count, batch_id, and entity
    values. It is used to simulate a state of a pipeline for testing purposes.

    Returns:
        PipelineState: An instance of PipelineState with predefined values.
    """
    state = PipelineState()
    state.errors = {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
    state.duration = 10
    state.count = {"count1": 1, "count2": 2}
    state.batch_id = 1
    state.entity = "Entity1"
    return state


class TestPipelineState:
    """
    A test class for verifying the functionality of the PipelineState class.

    This class tests the PipelineState's singleton pattern, initialization process, setter and getter methods, state
    resetting, and resume functionality.
    """
    def test_singleton(self, pipeline_state):
        """
        Test the singleton behavior of the PipelineState class.

        This test verifies that only one instance of PipelineState is created, ensuring that it follows the singleton
        design pattern.

        Args:
            pipeline_state (PipelineState): The PipelineState fixture.
        """
        state2 = PipelineState()
        assert pipeline_state is state2

    def test_initialization(self, pipeline_state):
        """
        Test the initialization of the PipelineState class.

        This test checks that PipelineState is correctly initialized with the provided state values.

        Args:
            pipeline_state (PipelineState): The PipelineState fixture.
        """
        assert pipeline_state.errors == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert pipeline_state.duration == 10
        assert pipeline_state.count == {"count1": 1, "count2": 2}
        assert pipeline_state.batch_id == 1
        assert pipeline_state.entity == "Entity1"

    def test_setters_getters(self, pipeline_state):
        """
        Test the setter and getter methods of the PipelineState class.

        This test verifies that PipelineState's attributes can be correctly set and retrieved.

        Args:
            pipeline_state (PipelineState): The PipelineState fixture.
        """
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
        """
        Test the reset functionality of the PipelineState class.

        This test checks that the reset method correctly resets the state of PipelineState to its initial values.

        Args:
            pipeline_state (PipelineState): The PipelineState fixture.
        """
        pipeline_state.reset()

        assert pipeline_state.errors == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert pipeline_state.duration == 0
        assert pipeline_state.count == {}
        assert pipeline_state.batch_id == 0
        assert pipeline_state.entity == ""

    def test_resume(self, pipeline_state):
        """
        Test the resume functionality of the PipelineState class.

        This test verifies that the resume method correctly returns a summary of the pipeline's state and operations.

        Args:
            pipeline_state (PipelineState): The PipelineState fixture.
        """
        resume = pipeline_state.resume()

        assert len(resume) == 1
        assert resume[0]["entity"] == "Entity1"
        assert resume[0]["operationMetrics"] == {"count1": 1, "count2": 2}
        assert resume[0]["errors"] == {"DataProcessing": "No errors", "PipelineExecution": "No errors"}
        assert resume[0]["batchId"] == 1
        assert resume[0]["duration"] == 10

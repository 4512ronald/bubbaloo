import pytest
from unittest.mock import MagicMock
from google.cloud.storage import Client, Bucket, Blob
from bubbaloo.services.cloud.gcp.storage import CloudStorageManager


@pytest.fixture
def project():
    return "test_project"


@pytest.fixture
def source():
    return "gs://test_bucket/test_prefix"


@pytest.fixture
def max_results_per_page():
    return 10


@pytest.fixture
def source_bucket():
    return Bucket(client=MagicMock(), name="source_bucket")


@pytest.fixture
def source_blob(source_bucket):
    return Blob(name="source_blob", bucket=source_bucket)


@pytest.fixture
def destination_bucket_name():
    return "destination_bucket"


@pytest.fixture
def destination_blob_name():
    return "destination_blob"


@pytest.fixture
def source_blob_paths():
    return ["gs://test_bucket/test_blob1", "gs://test_bucket/test_blob2"]


@pytest.fixture
def destination_gcs_path():
    return "gs://destination_bucket/destination_folder"


@pytest.fixture
def storage_client_mock():
    client = MagicMock(spec=Client)
    return client


@pytest.fixture
def cloud_storage_manager(project, storage_client_mock):
    manager = CloudStorageManager(project)
    manager._client = storage_client_mock
    return manager


class MockedIterator:
    def __init__(self, blobs):
        self.pages = [blobs]

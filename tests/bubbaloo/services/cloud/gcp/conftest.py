import pytest
from unittest.mock import MagicMock
from google.cloud.storage import Client, Bucket, Blob
from bubbaloo.services.cloud.gcp.storage import CloudStorageManager


@pytest.fixture
def project():
    """
    A pytest fixture that provides a mock project name.

    Returns:
        str: The name of the test project.
    """
    return "test_project"


@pytest.fixture
def source():
    """
    A pytest fixture that provides a mock source path in Google Cloud Storage.

    Returns:
        str: The source path in Google Cloud Storage.
    """
    return "gs://test_bucket/test_prefix"


@pytest.fixture
def max_results_per_page():
    """
    A pytest fixture that provides a mock maximum number of results per page.

    Returns:
        int: The maximum number of results that can appear on a page.
    """
    return 10


@pytest.fixture
def source_bucket():
    """
    A pytest fixture that provides a mock source bucket.

    Returns:
        Bucket: A mock Bucket instance representing the source bucket.
    """
    return Bucket(client=MagicMock(), name="source_bucket")


@pytest.fixture
def source_blob(source_bucket):
    """
    A pytest fixture that provides a mock source Blob object.

    Args:
        source_bucket (Bucket): The source bucket fixture.

    Returns:
        Blob: A mock Blob instance representing the source blob.
    """
    return Blob(name="source_blob", bucket=source_bucket)


@pytest.fixture
def destination_bucket_name():
    """
    A pytest fixture that provides a mock name for the destination bucket.

    Returns:
        str: The name of the destination bucket.
    """
    return "destination_bucket"


@pytest.fixture
def destination_blob_name():
    """
    A pytest fixture that provides a mock name for the destination blob.

    Returns:
        str: The name of the destination blob.
    """
    return "destination_blob"


@pytest.fixture
def source_blob_paths():
    """
    A pytest fixture that provides mock paths for source blobs.

    Returns:
        list: A list of strings representing the paths of source blobs.
    """
    return ["gs://test_bucket/test_blob1", "gs://test_bucket/test_blob2"]


@pytest.fixture
def destination_gcs_path():
    """
    A pytest fixture that provides a mock destination path in Google Cloud Storage.

    Returns:
        str: The destination path in Google Cloud Storage.
    """
    return "gs://destination_bucket/destination_folder"


@pytest.fixture
def storage_client_mock():
    """
    A pytest fixture that provides a mock Google Cloud Storage client.

    Returns:
        MagicMock: A mock of the Google Cloud Storage Client.
    """
    client = MagicMock(spec=Client)
    return client


@pytest.fixture
def cloud_storage_manager(project, storage_client_mock):
    """
    A pytest fixture that provides a mock CloudStorageManager instance.

    Args:
        project (str): The project fixture.
        storage_client_mock (Client): The mock storage client fixture.

    Returns:
        CloudStorageManager: A mock instance of the CloudStorageManager.
    """
    manager = CloudStorageManager(project)
    manager._client = storage_client_mock
    return manager


class MockedIterator:
    """
    A mock class to simulate an iterator for paginated results.

    This class is designed to mock the behavior of paginated results in APIs, such as those returned by Google Cloud
    Storage client methods.

    Attributes:
        pages (list): A list representing the pages of results.
    """
    def __init__(self, blobs):
        """
        Initializes a MockedIterator instance with a list of blobs.

        Args:
            blobs (list): A list of blobs to be used as the paginated results.
        """
        self.pages = [blobs]

import pytest
from unittest.mock import MagicMock
from google.cloud.storage import Bucket, Blob
from bubbaloo.services.cloud.gcp.storage import CloudStorageManager
from tests.bubbaloo.services.cloud.gcp.conftest import MockedIterator


class TestCloudStorageManager:
    """
    A test class for verifying the functionality of the CloudStorageManager.

    This class tests various operations of CloudStorageManager such as singleton behavior, listing objects in storage,
    handling invalid sources, bucket and object retrieval, copying, deleting, moving, and filtering blobs.
    """
    @pytest.fixture
    def storage_manager(self, project, storage_client_mock):
        """
        A pytest fixture that creates and returns a CloudStorageManager instance.

        Args:
            project (str): The project fixture.
            storage_client_mock (MagicMock): The mock storage client fixture.

        Returns:
            CloudStorageManager: An instance of CloudStorageManager.
        """
        manager = CloudStorageManager(project)
        manager._client = storage_client_mock
        return manager

    def test_singleton(self, storage_manager, project):
        """
        Test the singleton behavior of the CloudStorageManager.

        This test verifies that only one instance of CloudStorageManager is created for the same project.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            project (str): The project name.
        """
        new_manager = CloudStorageManager(project)
        assert new_manager is storage_manager

    def test_list(self, storage_manager, storage_client_mock):
        """
        Test the listing functionality of the CloudStorageManager.

        This test checks the ability of the CloudStorageManager to list blobs in a specified Google Cloud Storage path.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            storage_client_mock (MagicMock): The mock storage client.
        """
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_client_mock.list_blobs.return_value = MockedIterator([MagicMock(spec=Blob), MagicMock(spec=Blob)])
        blobs = storage_manager.list("gs://test_bucket/test_prefix")
        assert len(blobs) == 2
        storage_client_mock.list_blobs.assert_called_once_with(
            storage_client_mock.get_bucket.return_value,
            prefix="test_prefix/",
            max_results=100
        )

    def test_list_invalid_source(self, storage_manager):
        """
        Test listing functionality with an invalid source in the CloudStorageManager.

        This test verifies that the CloudStorageManager raises an appropriate exception when an invalid source path is
        provided.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
        """
        with pytest.raises(ValueError):
            storage_manager.list("invalid_source")

    def test_get_bucket_and_object(self, storage_manager, storage_client_mock):
        """
        Test retrieval of bucket and object from a path in the CloudStorageManager.

        This test checks the CloudStorageManager's ability to correctly parse and retrieve bucket and object information
        from a given Google Cloud Storage path.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            storage_client_mock (MagicMock): The mock storage client.
        """
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        bucket, blob = storage_manager._get_bucket_and_object("gs://test_bucket/test_blob")
        assert isinstance(bucket, MagicMock)
        assert isinstance(blob, MagicMock)

    def test_get_bucket_and_object_invalid_path(self, storage_manager):
        """
        Test retrieval of bucket and object with an invalid path in the CloudStorageManager.

        This test verifies that the CloudStorageManager raises an appropriate exception when an invalid path is
        provided.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
        """
        with pytest.raises(ValueError):
            storage_manager._get_bucket_and_object("invalid_path")

    def test_copy(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        """
        Test the copy functionality of the CloudStorageManager.

        This test checks the CloudStorageManager's ability to copy a blob from one location to another within Google
        Cloud Storage.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            storage_client_mock (MagicMock): The mock storage client.
            source_bucket (Bucket): The source bucket fixture.
            source_blob (Blob): The source blob fixture.
        """
        source_bucket.copy_blob = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager._copy(source_bucket, source_blob, "destination_bucket", "destination_blob")
        source_bucket.copy_blob.assert_called_once()
        storage_client_mock.get_bucket.assert_called_once_with("destination_bucket")

    def test_delete(self, storage_manager, source_blob):
        """
        Test the delete functionality of the CloudStorageManager.

        This test verifies the CloudStorageManager's ability to delete a blob in Google Cloud Storage.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            source_blob (Blob): The source blob fixture.
        """
        source_blob.delete = MagicMock()
        storage_manager.delete(source_blob)
        source_blob.delete.assert_called_once_with(if_generation_match=source_blob.generation)

    def test_move(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        """
        Test the move functionality of the CloudStorageManager.

        This test checks the CloudStorageManager's ability to move a blob from one location to another within Google
        Cloud Storage.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            storage_client_mock (MagicMock): The mock storage client.
            source_bucket (Bucket): The source bucket fixture.
            source_blob (Blob): The source blob fixture.
        """
        source_bucket.copy_blob = MagicMock()
        source_blob.delete = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager._get_bucket_and_object = MagicMock(return_value=(source_bucket, source_blob))
        storage_manager.move(["gs://test_bucket/test_blob"], "gs://destination_bucket/destination_folder")
        source_bucket.copy_blob.assert_called_once()
        source_blob.delete.assert_called_once()
        storage_client_mock.get_bucket.assert_called_once_with("destination_bucket")

    def test_workflow(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        """
        Test a complete workflow in the CloudStorageManager.

        This test covers a full cycle of operations including listing, moving, and copying blobs within Google Cloud
        Storage using the CloudStorageManager.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
            storage_client_mock (MagicMock): The mock storage client.
            source_bucket (Bucket): The source bucket fixture.
            source_blob (Blob): The source blob fixture.
        """
        source_bucket.copy_blob = MagicMock()
        source_blob.delete = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager._get_bucket_and_object = MagicMock(return_value=(source_bucket, source_blob))
        storage_client_mock.list_blobs.return_value = MockedIterator([source_blob])

        blobs = storage_manager.list("gs://test_bucket/test_prefix")
        assert len(blobs) == 1
        storage_client_mock.list_blobs.assert_called_once_with(
            storage_client_mock.get_bucket.return_value,
            prefix="test_prefix/",
            max_results=100
        )

        storage_manager.move(["gs://test_bucket/test_blob"], "gs://destination_bucket/destination_folder")
        source_bucket.copy_blob.assert_called_once()
        source_blob.delete.assert_called_once()
        assert storage_client_mock.get_bucket.call_count == 2
        storage_client_mock.get_bucket.assert_any_call("test_bucket")
        storage_client_mock.get_bucket.assert_any_call("destination_bucket")

    def test_filter(self, storage_manager):
        """
        Test the filter functionality of the CloudStorageManager.

        This test verifies the CloudStorageManager's ability to filter blobs in Google Cloud Storage based on a provided
        filter function.

        Args:
            storage_manager (CloudStorageManager): The storage manager fixture.
        """
        blob1 = MagicMock(spec=Blob)
        blob1.name = "blob1"
        blob2 = MagicMock(spec=Blob)
        blob2.name = "blob2"
        blob3 = MagicMock(spec=Blob)
        blob3.name = "blob3"

        blobs = [blob1, blob2, blob3]

        def filter_func(blob):
            return blob.name if blob.name != "blob2" else None

        filtered_blobs = storage_manager.filter(blobs, filter_func)

        assert len(filtered_blobs) == 2
        assert blob1.name in filtered_blobs
        assert blob2.name not in filtered_blobs
        assert blob3.name in filtered_blobs

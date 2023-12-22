import pytest
from unittest.mock import MagicMock
from google.cloud.storage import Bucket, Blob
from bubbaloo.services.cloud.gcp.storage import CloudStorageManager
from tests.bubbaloo.services.cloud.gcp.conftest import MockedIterator


class TestCloudStorageManager:
    @pytest.fixture
    def storage_manager(self, project, storage_client_mock):
        manager = CloudStorageManager(project)
        manager._client = storage_client_mock
        return manager

    def test_singleton(self, storage_manager, project):
        new_manager = CloudStorageManager(project)
        assert new_manager is storage_manager

    def test_list(self, storage_manager, storage_client_mock):
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_client_mock.list_blobs.return_value = MockedIterator([MagicMock(spec=Blob), MagicMock(spec=Blob)])
        blobs = storage_manager.list("gs://test_bucket/test_prefix")
        assert len(blobs) == 2
        storage_client_mock.list_blobs.assert_called_once_with(storage_client_mock.get_bucket.return_value,
                                                               prefix="test_prefix/", max_results=100)

    def test_list_invalid_source(self, storage_manager):
        with pytest.raises(ValueError):
            storage_manager.list("invalid_source")

    def test_get_bucket_and_object(self, storage_manager, storage_client_mock):
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        bucket, blob = storage_manager._get_bucket_and_object("gs://test_bucket/test_blob")
        assert isinstance(bucket, MagicMock)
        assert isinstance(blob, MagicMock)

    def test_get_bucket_and_object_invalid_path(self, storage_manager):
        with pytest.raises(ValueError):
            storage_manager._get_bucket_and_object("invalid_path")

    def test_copy(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        source_bucket.copy_blob = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager.copy(source_bucket, source_blob, "destination_bucket", "destination_blob")
        source_bucket.copy_blob.assert_called_once()
        storage_client_mock.get_bucket.assert_called_once_with("destination_bucket")

    def test_delete(self, storage_manager, source_blob):
        source_blob.delete = MagicMock()
        storage_manager.delete(source_blob)
        source_blob.delete.assert_called_once_with(if_generation_match=source_blob.generation)

    def test_move(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        source_bucket.copy_blob = MagicMock()
        source_blob.delete = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager._get_bucket_and_object = MagicMock(return_value=(source_bucket, source_blob))
        storage_manager.move(["gs://test_bucket/test_blob"], "gs://destination_bucket/destination_folder")
        source_bucket.copy_blob.assert_called_once()
        source_blob.delete.assert_called_once()
        storage_client_mock.get_bucket.assert_called_once_with("destination_bucket")

    def test_workflow(self, storage_manager, storage_client_mock, source_bucket, source_blob):
        source_bucket.copy_blob = MagicMock()
        source_blob.delete = MagicMock()
        storage_client_mock.get_bucket.return_value = MagicMock(spec=Bucket)
        storage_manager._get_bucket_and_object = MagicMock(return_value=(source_bucket, source_blob))
        storage_client_mock.list_blobs.return_value = MockedIterator([source_blob])

        blobs = storage_manager.list("gs://test_bucket/test_prefix")
        assert len(blobs) == 1
        storage_client_mock.list_blobs.assert_called_once_with(storage_client_mock.get_bucket.return_value,
                                                               prefix="test_prefix/", max_results=100)

        storage_manager.move(["gs://test_bucket/test_blob"], "gs://destination_bucket/destination_folder")
        source_bucket.copy_blob.assert_called_once()
        source_blob.delete.assert_called_once()
        assert storage_client_mock.get_bucket.call_count == 2
        storage_client_mock.get_bucket.assert_any_call("test_bucket")
        storage_client_mock.get_bucket.assert_any_call("destination_bucket")
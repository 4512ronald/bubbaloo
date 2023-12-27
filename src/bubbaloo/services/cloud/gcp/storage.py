import os
from typing import List, Tuple, Callable
import re

from bubbaloo.utils.interfaces.storage_client import IStorageManager

from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket


class CloudStorageManager(IStorageManager):
    """
    A class for managing storage operations in Google Cloud Storage.

    This class implements the IStorageManager interface and provides methods for
    interacting with Google Cloud Storage. It allows for listing, copying, deleting,
    and moving files within Google Cloud Storage.

    Attributes:
        _instance: Singleton instance of CloudStorageManager.
        project (str): The Google Cloud project associated with the storage.
        _client (Client): The Google Cloud Storage client.
        _bucket (Bucket | None): The current Google Cloud Storage bucket.
        _initialized (bool): A flag indicating whether the instance is initialized.
    """
    _instance = None

    def __new__(cls, project: str, credentials: str = None, **kwargs):
        """
        Creates a new instance of the class or returns the existing one.

        Ensures that only one instance of CloudStorageManager is created per project.

        Args:
            project (str): The Google Cloud project to associate with the storage manager.

        Returns:
            CloudStorageManager: The singleton instance of the class.
        """
        if not cls._instance:
            cls._instance = super(CloudStorageManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, project: str, credentials: str = None, **kwargs) -> None:
        """
        Initializes the CloudStorageManager instance with a specific Google Cloud project.

        Args:
            project (str): The Google Cloud project to associate with the storage manager.
        """
        if not self._initialized:
            self.project: str = project
            if credentials is None:
                self._client: Client = storage.Client(self.project, **kwargs)
            else:
                self._client: Client = storage.Client.from_service_account_json(credentials)
            self._bucket: Bucket | None = None
            self._initialized = True

    def list(self, source: str, max_results_per_page: int = 100) -> List[Blob]:
        """
        Lists blobs in a Google Cloud Storage bucket.

        Args:
            source (str): The Google Cloud Storage bucket path (e.g., 'gs://bucket-name/prefix').
            max_results_per_page (int, optional): Maximum number of results to return per page.

        Returns:
            List[Blob]: A list of Blob objects in the specified bucket.

        Raises:
            ValueError: If the source path is invalid.
        """
        match = re.match(r"gs://([a-z0-9_\-.]+)/(.+)", source)
        if not match:
            raise ValueError(f"Invalid source path: {source}")

        bucket_name, prefix = match.groups()
        self._bucket = self._client.get_bucket(bucket_name)
        prefix = f"{prefix}/"

        blobs = []
        iterator = self._client.list_blobs(self._bucket, prefix=prefix, max_results=max_results_per_page)

        for page in iterator.pages:
            blobs.extend(page)

        return blobs

    def _get_bucket_and_object(self, path: str) -> Tuple[Bucket, Blob]:
        """
        Retrieves the bucket and blob object for a given Google Cloud Storage path.

        Args:
            path (str): The full path to the blob (e.g., 'gs://bucket-name/object-name').

        Returns:
            Tuple[Bucket, Blob]: The bucket and blob object corresponding to the path.
        """
        if "gs://" not in path:
            raise ValueError(f"Invalid path: {path}")
        parts = path.replace("gs://", "").split("/")
        bucket_name = parts[0]
        blob_name = "/".join(parts[1:])

        try:
            bucket = self._client.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)
        except Exception as e:
            raise ValueError(f"Invalid bucket name or blob name: {e}") from e

        return bucket, blob

    def _copy(
            self,
            source_bucket: Bucket,
            source_blob: Blob,
            destination_bucket_name: str,
            destination_blob_name: str
    ) -> None:
        """
        Copies a blob from one bucket to another in Google Cloud Storage.

        Args:
            source_bucket (Bucket): The source bucket.
            source_blob (Blob): The blob to copy.
            destination_bucket_name (str): The name of the destination bucket.
            destination_blob_name (str): The name for the blob in the destination bucket.
        """
        destination_bucket = self._client.get_bucket(destination_bucket_name)

        source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_blob_name,
        )

    @staticmethod
    def delete(source_blob: Blob) -> None:
        """
        Deletes a blob from Google Cloud Storage.

        Args:
            source_blob (Blob): The blob to delete.
        """
        source_blob.delete(if_generation_match=source_blob.generation)

    def move(self, source_blob_paths: List[str], destination_gcs_path: str) -> None:
        """
        Moves a list of blobs to a specified location within Google Cloud Storage.

        Args:
            source_blob_paths (List[str]): A list of paths for the blobs to move.
            destination_gcs_path (str): The destination path in Google Cloud Storage.
        """
        destination_path_tuple: tuple[str, str] = re.findall(r"gs://([a-z0-9_\-.]+)/(.+)", destination_gcs_path)[0]
        destination_bucket_name: str = destination_path_tuple[0]
        destination_folder: str = f"{destination_path_tuple[1]}/"

        for source_blob_path in source_blob_paths:
            source_bucket, source_blob = self._get_bucket_and_object(source_blob_path)
            destination_blob_name = f'{destination_folder.rstrip("/")}/{source_blob.name.split("/")[-1]}'

            self._copy(source_bucket, source_blob, destination_bucket_name, destination_blob_name)
            self.delete(source_blob)

    def filter(self, blobs: List[Blob], filter_func: Callable[..., str]) -> List[str]:
        """
        Filters a list of Blob objects based on a specified filter function.

        This method applies a filter function to each Blob in the given list and returns a list of results for which
        the filter function does not return None.

        Args:
            blobs (List[Blob]): A list of Blob objects to be filtered.
            filter_func (Callable[..., str]): A function that takes a Blob object as input and returns a string if the
            Blob meets the filter criteria, or None otherwise.

        Returns:
            List[str]: A list of results from the filter function for Blobs that meet the criteria.
        """
        return [blob for blob in map(lambda blob: filter_func(blob), blobs) if blob is not None]

    def copy(self, source_blob_paths: List[str], destination_path: str) -> None:
        """
        Copies or downloads blobs based on the destination path.

        Args:
            source_blob_paths (List[str]): A list of paths for the blobs.
            destination_path (str): The destination path, either a GCS path or a local path.
        """
        if destination_path.startswith("gs://"):
            destination_bucket_name, destination_folder = re.findall(r"gs://([a-z0-9_\-.]+)/(.+)", destination_path)[0]
            destination_folder = f"{destination_folder}/" if not destination_folder.endswith(
                "/") else destination_folder

            for source_blob_path in source_blob_paths:
                source_bucket, source_blob = self._get_bucket_and_object(source_blob_path)
                destination_blob_name = f'{destination_folder.rstrip("/")}/{source_blob.name.split("/")[-1]}'
                self._copy(source_bucket, source_blob, destination_bucket_name, destination_blob_name)
        else:
            for source_blob_path in source_blob_paths:
                source_bucket, source_blob = self._get_bucket_and_object(source_blob_path)
                destination_file_path = os.path.join(destination_path, source_blob.name.split("/")[-1])

                try:
                    source_blob.download_to_filename(destination_file_path)
                except Exception as e:
                    raise ValueError(f"Error downloading blob {source_blob.name}: {e}") from e

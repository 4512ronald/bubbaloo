from typing import List
import re

from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket


class GCSClient:
    """
    Client for interacting with Google Cloud Storage.

    Args:
        project (str): The GCP project ID.

    Attributes:
        project (str): The GCP project ID.
        _client (google.cloud.storage.client.Client | None): The GCS client.
        _bucket (google.cloud.storage.bucket.Bucket | None): The GCS bucket.

    """

    _instance = None

    def __init__(self, project: str) -> None:
        self.project: str = project
        self._client: Client | None = None
        self._bucket: Bucket | None = None

    def _init_client(self) -> None:
        """Initialize the GCS client."""
        if not self._client:
            self._client = storage.Client(self.project)

    def get_blobs(self, source: str, max_results_per_page: int = 100) -> List[Blob]:
        """
        Get a list of GCS blobs.

        Args:
            source (str): The GCS source path (e.g., "gs://bucket-name/path/to/files").
            max_results_per_page (int): The maximum number of results per page (default: 100).

        Returns:
            List[Blob]: List of GCS blobs.

        """
        self._init_client()

        path_tuple: tuple[str, str] = re.findall(r"gs://([a-z0-9_\-.]+)/(.+)", source)[0]
        bucket_name: str = path_tuple[0]
        self._bucket = self._client.get_bucket(bucket_name)
        prefix = f"{path_tuple[1]}/"

        blobs = []
        iterator = self._client.list_blobs(self._bucket, prefix=prefix, max_results=max_results_per_page)

        for page in iterator.pages:
            blobs.extend(page)

        return blobs

    def move_blobs(self, source_blob_paths: List[str], destination_gcs_path: str):
        """
        Move blobs from one folder to another within the same or different bucket.

        Args:
            source_blob_paths (List[str]): List of GCS source blob paths (e.g., "gs://bucket-name/path/to/file").
            destination_gcs_path (str): The GCS destination path (e.g., "gs://bucket-name/path/to/destination").
        """
        self._init_client()
        destination_generation_match_precondition = 0

        destination_path_tuple: tuple[str, str] = re.findall(r"gs://([a-z0-9_\-.]+)/(.+)", destination_gcs_path)[0]
        destination_bucket_name: str = destination_path_tuple[0]
        destination_folder: str = f"{destination_path_tuple[1]}/"

        for source_blob_path in source_blob_paths:
            parts = source_blob_path.replace("gs://", "").split("/")
            bucket_name = parts[0]
            blob_name = "/".join(parts[1:])

            source_bucket = self._client.get_bucket(bucket_name)
            source_blob = source_bucket.blob(blob_name)

            destination_bucket = self._client.get_bucket(destination_bucket_name)
            destination_blob_name = f'{destination_folder.rstrip("/")}/{blob_name.split("/")[-1]}'

            source_bucket.copy_blob(
                source_blob,
                destination_bucket,
                destination_blob_name,
                if_generation_match=destination_generation_match_precondition
            )

            source_blob.delete(if_generation_match=source_blob.generation)

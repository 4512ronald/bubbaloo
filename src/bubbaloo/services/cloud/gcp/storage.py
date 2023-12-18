from typing import List, Tuple
import re

from bubbaloo.utils.interfaces.storage_client import IStorageManager

from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client
from google.cloud.storage.bucket import Bucket


class CloudStorageManager(IStorageManager):

    _instance = None

    def __new__(cls, project: str):
        if not cls._instance:
            cls._instance = super(CloudStorageManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, project: str) -> None:

        if not self._initialized:
            self.project: str = project
            self._client: Client = storage.Client(self.project)
            self._bucket: Bucket | None = None
            self._initialized = True

    def list(self, source: str, max_results_per_page: int = 100) -> List[Blob]:

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
        parts = path.replace("gs://", "").split("/")
        bucket_name = parts[0]
        blob_name = "/".join(parts[1:])

        bucket = self._client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        return bucket, blob

    def copy(
            self,
            source_bucket: Bucket,
            source_blob: Blob,
            destination_bucket_name: str,
            destination_blob_name: str
    ) -> None:
        destination_generation_match_precondition = 0
        destination_bucket = self._client.get_bucket(destination_bucket_name)

        source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_blob_name,
            if_generation_match=destination_generation_match_precondition
        )

    @staticmethod
    def delete(source_blob: Blob) -> None:
        source_blob.delete(if_generation_match=source_blob.generation)

    def move(self, source_blob_paths: List[str], destination_gcs_path: str) -> None:
        destination_path_tuple: tuple[str, str] = re.findall(r"gs://([a-z0-9_\-.]+)/(.+)", destination_gcs_path)[0]
        destination_bucket_name: str = destination_path_tuple[0]
        destination_folder: str = f"{destination_path_tuple[1]}/"

        for source_blob_path in source_blob_paths:
            source_bucket, source_blob = self._get_bucket_and_object(source_blob_path)
            destination_blob_name = f'{destination_folder.rstrip("/")}/{source_blob.name.split("/")[-1]}'

            self.copy(source_bucket, source_blob, destination_bucket_name, destination_blob_name)
            self.delete(source_blob)

import os
from datetime import datetime, timezone, timedelta

from google.cloud.storage import Blob


def get_blobs_days_ago(blob: Blob, time_delta: float) -> str | None:
    """
    Filters a Blob object to determine if it was updated within the specified number of days.

    Checks if the update date of the given Blob is within a specified time delta from the current date.
    It excludes blobs that end with '/_SUCCESS' or '/' (typically directories or success markers).

    Args:
        blob (Blob): The Blob object to check.
        time_delta (float): The number of days to look back from today.

    Returns:
        str | None: The full path of the blob if it matches the criteria, otherwise None.
    """
    days_ago = datetime.now(timezone.utc).date() - timedelta(days=time_delta)

    if blob.updated.date() >= days_ago and not (blob.name.endswith("/_SUCCESS") or blob.name.endswith("/")):
        return f"gs://{blob.bucket.name}/{blob.name}"


def get_files_days_ago(file_path: str, time_delta: float) -> str | None:
    """
    Determines if a file was modified within the specified number of days.

    Checks if the modification date of the file at the given path is within a specified time delta
    from the current date.

    Args:
        file_path (str): The path of the file to check.
        time_delta (float): The number of days to look back from today.

    Returns:
        str | None: The file path if it was modified within the specified time delta, otherwise None.
    """
    mod_time = os.path.getmtime(file_path)
    mod_date = datetime.fromtimestamp(mod_time).date()

    days_ago = datetime.now().date() - timedelta(days=time_delta)

    if mod_date >= days_ago and not file_path.endswith("/_SUCCESS") or file_path.endswith("/"):
        return file_path

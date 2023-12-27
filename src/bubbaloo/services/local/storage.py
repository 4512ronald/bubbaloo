import os
import shutil
from typing import List, Callable

from bubbaloo.utils.interfaces.storage_client import IStorageManager


class LocalFileManager(IStorageManager):
    """
    Implementation of the IStorageManager interface for local file management.

    This class provides methods for basic file management operations such as listing,
    copying, deleting, and moving files in a local file system. It is designed to work
    with local file paths.
    """

    def list(self, source: str) -> List[str]:
        """
        Lists all files in the specified directory.

        Args:
            source (str): The directory whose files are to be listed.

        Returns:
            List[str]: A list of file paths in the specified directory.

        Raises:
            ValueError: If the specified source path is not a valid directory.
        """
        if not os.path.isdir(source):
            raise ValueError(f"Invalid source path: {source}")

        files = [os.path.join(source, f) for f in os.listdir(source)]
        return files

    @staticmethod
    def _copy(source_path: str, destination_path: str) -> None:
        """
        Copies a file from the source path to the destination path.

        Args:
            source_path (str): The path of the file to copy.
            destination_path (str): The path to copy the file to.
        """
        shutil.copy2(source_path, destination_path)

    def copy(self, source_files: List[str], destination_path: str) -> None:

        for source_path in source_files:
            self._copy(source_path, destination_path)

    @staticmethod
    def delete(file_path: str) -> None:
        """
        Deletes the file or directory at the specified path.

        Args:
            file_path (str): The path of the file or directory to delete.
        """
        if os.path.isdir(file_path):
            shutil.rmtree(file_path)
        else:
            os.remove(file_path)

    def move(self, source_files: List[str], destination_directory: str) -> None:
        """
        Moves a list of files to the specified destination directory.

        If the destination directory does not exist, it is created.

        Args:
            source_files (List[str]): A list of file paths to move.
            destination_directory (str): The directory to move the files to.
        """
        if not os.path.exists(destination_directory):
            os.makedirs(destination_directory)

        for file_path in source_files:
            if not os.path.exists(file_path):
                print(f"Source file does not exist: {file_path}")
                continue
            try:
                file_name = os.path.basename(file_path)
                destination_path = os.path.join(destination_directory, file_name)
                shutil.move(file_path, destination_path)
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Failed to move file {file_path} to {destination_directory}. Error: {e}")

    def filter(self, blobs: List[str], filter_func: Callable[..., str]) -> List[str]:
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

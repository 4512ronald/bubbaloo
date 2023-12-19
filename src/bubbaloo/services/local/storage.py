import os
import shutil
from typing import List

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

    def copy(self, source_path: str, destination_path: str) -> None:
        """
        Copies a file from the source path to the destination path.

        Args:
            source_path (str): The path of the file to copy.
            destination_path (str): The path to copy the file to.
        """
        shutil.copy2(source_path, destination_path)

    @staticmethod
    def delete(file_path: str) -> None:
        """
        Deletes the file at the specified path.

        Args:
            file_path (str): The path of the file to delete.
        """
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
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(destination_directory, file_name)
            shutil.move(file_path, destination_path)

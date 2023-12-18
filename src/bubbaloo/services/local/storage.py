import os
import shutil
from typing import List

from bubbaloo.utils.interfaces.storage_client import IStorageManager


class LocalFileManager(IStorageManager):

    def list(self, source: str) -> List[str]:
        """
        List files in a local directory.
        """
        if not os.path.isdir(source):
            raise ValueError(f"Invalid source path: {source}")

        files = [os.path.join(source, f) for f in os.listdir(source)]
        return files

    def copy(self, source_path: str, destination_path: str) -> None:
        """
        Copy a file from source to destination.
        """
        shutil.copy2(source_path, destination_path)

    @staticmethod
    def delete(file_path: str) -> None:
        """
        Delete a file.
        """
        os.remove(file_path)

    def move(self, source_files: List[str], destination_directory: str) -> None:
        """
        Move a list of files to a destination directory.
        """
        if not os.path.exists(destination_directory):
            os.makedirs(destination_directory)

        for file_path in source_files:
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(destination_directory, file_name)
            shutil.move(file_path, destination_path)

import os
import pytest
from bubbaloo.services.local.storage import LocalFileManager


class TestLocalFileManager:
    """
    A test class for verifying the functionality of the LocalFileManager.

    This class tests various operations of LocalFileManager such as listing, copying, deleting, moving, and filtering
    files in a local file system.
    """
    @pytest.fixture
    def source_directory(self, tmp_path):
        """
        A pytest fixture that creates and returns a temporary source directory for testing.

        Args:
            tmp_path (Path): A fixture that provides a temporary directory.

        Returns:
            str: The path to the created temporary source directory.
        """
        directory = tmp_path / "source"
        directory.mkdir()
        return str(directory)

    @pytest.fixture
    def destination_directory(self, tmp_path):
        """
        A pytest fixture that creates and returns a temporary destination directory for testing.

        Args:
            tmp_path (Path): A fixture that provides a temporary directory.

        Returns:
            str: The path to the created temporary destination directory.
        """
        directory = tmp_path / "destination"
        directory.mkdir()
        return str(directory)

    @pytest.fixture
    def source_files(self, source_directory):
        """
        A pytest fixture that creates test files in the source directory and returns their paths.

        Args:
            source_directory (str): The source directory where test files will be created.

        Returns:
            list: A list of paths to the created test files.
        """
        files = []
        for i in range(3):
            file_path = os.path.join(source_directory, f"file{i}.txt")
            with open(file_path, "w") as f:
                f.write("test")
            files.append(file_path)
        return files

    @pytest.fixture
    def file_manager(self):
        """
        A pytest fixture that creates and returns an instance of LocalFileManager.

        Returns:
            LocalFileManager: An instance of LocalFileManager.
        """
        return LocalFileManager()

    def test_list(self, file_manager, source_directory, source_files):
        """
        Test the file listing functionality of the LocalFileManager.

        This test verifies that LocalFileManager can correctly list files in a specified directory.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
            source_directory (str): The source directory containing test files.
            source_files (list): The list of test file paths in the source directory.
        """
        files = file_manager.list(source_directory)
        assert len(files) == 3
        assert set(files) == set(source_files)

    def test_copy(self, file_manager, source_directory, destination_directory):
        """
        Test the file copying functionality of the LocalFileManager.

        This test checks the ability of LocalFileManager to copy a file from a source to a destination directory.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
            source_directory (str): The source directory containing the file to copy.
            destination_directory (str): The destination directory where the file will be copied.
        """
        source_file = os.path.join(source_directory, "file.txt")
        destination_file = os.path.join(destination_directory, "file.txt")
        with open(source_file, "w") as f:
            f.write("test")
        file_manager._copy(source_file, destination_file)
        assert os.path.exists(destination_file)

    def test_delete(self, file_manager, source_directory):
        """
        Test the file deletion functionality of the LocalFileManager.

        This test verifies the ability of LocalFileManager to delete a file from a directory.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
            source_directory (str): The source directory containing the file to delete.
        """
        file_path = os.path.join(source_directory, "file.txt")
        with open(file_path, "w") as f:
            f.write("test")
        file_manager.delete(file_path)
        assert not os.path.exists(file_path)

    def test_move(self, file_manager, source_files, destination_directory):
        """
        Test the file moving functionality of the LocalFileManager.

        This test checks LocalFileManager's ability to move files from a source directory to a destination directory.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
            source_files (list): The list of test file paths in the source directory.
            destination_directory (str): The destination directory where files will be moved.
        """
        file_manager.move(source_files, destination_directory)
        for file_path in source_files:
            assert not os.path.exists(file_path)
            destination_file_path = os.path.join(destination_directory, os.path.basename(file_path))
            assert os.path.exists(destination_file_path)

    def test_workflow(self, file_manager, source_directory, destination_directory, source_files):
        """
        Test a complete file management workflow in the LocalFileManager.

        This test covers a full cycle of operations including listing, moving, copying, and deleting files using the
         LocalFileManager.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
            source_directory (str): The source directory containing test files.
            destination_directory (str): The destination directory for file operations.
            source_files (list): The list of test file paths in the source directory.
        """
        files = file_manager.list(source_directory)
        assert len(files) == len(source_files)
        assert set(files) == set(source_files)

        for file_path in source_files:
            destination_file_path = os.path.join(destination_directory, os.path.basename(file_path))
            file_manager.move([file_path], destination_file_path)
            assert not os.path.exists(file_path)
            assert os.path.exists(destination_file_path)

        files = file_manager.list(destination_directory)
        for file_path in files:
            file_manager.delete(file_path)
            assert not os.path.exists(file_path)

    def test_filter(self, file_manager):
        """
        Test the file filtering functionality of the LocalFileManager.

        This test verifies LocalFileManager's ability to filter files based on a provided filter function.

        Args:
            file_manager (LocalFileManager): The file manager fixture.
        """
        file1 = "/path/to/file1"
        file2 = "/path/to/file2"
        file3 = "/path/to/file3"

        files = [file1, file2, file3]

        def filter_func(file):
            return file if file != "/path/to/file2" else None

        filtered_files = file_manager.filter(files, filter_func)

        assert len(filtered_files) == 2
        assert file1 in filtered_files
        assert file2 not in filtered_files
        assert file3 in filtered_files

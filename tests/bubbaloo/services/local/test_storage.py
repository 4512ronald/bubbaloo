import os
import pytest
from bubbaloo.services.local.storage import LocalFileManager


class TestLocalFileManager:
    @pytest.fixture
    def source_directory(self, tmp_path):
        directory = tmp_path / "source"
        directory.mkdir()
        return str(directory)

    @pytest.fixture
    def destination_directory(self, tmp_path):
        directory = tmp_path / "destination"
        directory.mkdir()
        return str(directory)

    @pytest.fixture
    def source_files(self, source_directory):
        files = []
        for i in range(3):
            file_path = os.path.join(source_directory, f"file{i}.txt")
            with open(file_path, "w") as f:
                f.write("test")
            files.append(file_path)
        return files

    @pytest.fixture
    def file_manager(self):
        return LocalFileManager()

    def test_list(self, file_manager, source_directory, source_files):
        files = file_manager.list(source_directory)
        assert len(files) == 3
        assert set(files) == set(source_files)

    def test_copy(self, file_manager, source_directory, destination_directory):
        source_file = os.path.join(source_directory, "file.txt")
        destination_file = os.path.join(destination_directory, "file.txt")
        with open(source_file, "w") as f:
            f.write("test")
        file_manager.copy(source_file, destination_file)
        assert os.path.exists(destination_file)

    def test_delete(self, file_manager, source_directory):
        file_path = os.path.join(source_directory, "file.txt")
        with open(file_path, "w") as f:
            f.write("test")
        file_manager.delete(file_path)
        assert not os.path.exists(file_path)

    def test_move(self, file_manager, source_files, destination_directory):
        file_manager.move(source_files, destination_directory)
        for file_path in source_files:
            assert not os.path.exists(file_path)
            destination_file_path = os.path.join(destination_directory, os.path.basename(file_path))
            assert os.path.exists(destination_file_path)

    def test_workflow(self, file_manager, source_directory, destination_directory, source_files):
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

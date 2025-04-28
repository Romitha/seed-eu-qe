import shutil
import unittest
from pathlib import Path

from utils.common.path_util import (construct_path,
                                    convert_underscore_to_nested_path)


class TestCommonPathUtil(unittest.TestCase):

    scratch_dir = None

    @classmethod
    def setUpClass(cls):
        # Create the scratch_dir if it doesn't exist
        cls.scratch_dir = (
            Path(__file__).resolve().parents[3] / "unittests" / "scratch_dir"
        )
        cls.scratch_dir.mkdir(exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        # Clean up the scratch_dir after tests
        if cls.scratch_dir.exists():
            shutil.rmtree(cls.scratch_dir)

    def test_construct_path(self):
        # Test joining multiple path components
        path = construct_path("folder", "subfolder", "file.txt")
        expected_path = Path("folder/subfolder/file.txt")
        self.assertEqual(path, expected_path)

        # Test joining with existing Path objects
        path = construct_path(Path("folder"), "subfolder", Path("file.txt"))
        self.assertEqual(path, expected_path)

        # Test with no arguments
        with self.assertRaises(ValueError):
            construct_path()

        # Test with invalid argument types
        with self.assertRaises(TypeError):
            construct_path("folder", 123, "file.txt")
        with self.assertRaises(TypeError):
            construct_path("folder", None, "file.txt")

    def test_convert_underscore_to_nested_path(self):
        # Test converting underscore-separated string to path
        path = convert_underscore_to_nested_path("folder_subfolder_file.txt")
        expected_path = Path("folder/subfolder/file.txt")
        self.assertEqual(path, expected_path)

        # Test converting Path object with underscores
        path = convert_underscore_to_nested_path(Path("folder_subfolder_file.txt"))
        self.assertEqual(path, expected_path)

        # Test with empty string
        with self.assertRaises(ValueError):
            convert_underscore_to_nested_path("")

        # Test with empty Path object
        with self.assertRaises(ValueError):
            convert_underscore_to_nested_path(Path(""))

        # Test with invalid input types
        with self.assertRaises(TypeError):
            convert_underscore_to_nested_path(None)
        with self.assertRaises(TypeError):
            convert_underscore_to_nested_path(123)

    def test_scratch_dir_usage(self):
        # Create a dummy file in the scratch_dir
        dummy_file_path = self.scratch_dir / "dummy_file.txt"
        with dummy_file_path.open("w") as file:
            file.write("dummy content")

        # Check if the file was created
        self.assertTrue(dummy_file_path.exists())

        # Clean up the dummy file
        dummy_file_path.unlink()
        self.assertFalse(dummy_file_path.exists())

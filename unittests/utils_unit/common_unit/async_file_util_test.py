import json
import unittest
from pathlib import Path

import aiofiles

# Import functions from your module
from utils.common.async_util import (async_load_file_in_path,
                                     async_load_json_file_in_path,
                                     async_load_multiple_files)


class TestAsyncFileUtils(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """Set up test directory and files asynchronously"""
        self.scratch_dir = Path("unittests/scratch_dir")
        self.scratch_dir.mkdir(parents=True, exist_ok=True)
        self.test_json_file = self.scratch_dir / "test.json"
        self.test_invalid_json_file = self.scratch_dir / "invalid.json"
        self.test_missing_file = self.scratch_dir / "missing.json"
        self.test_unsupported_file = self.scratch_dir / "test.txt"

        # Create valid JSON file
        async with aiofiles.open(self.test_json_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps({"key": "value"}))

        # Create invalid JSON file
        async with aiofiles.open(self.test_invalid_json_file, "w", encoding="utf-8") as f:
            await f.write("invalid json data")

        # Create unsupported format file
        async with aiofiles.open(self.test_unsupported_file, "w", encoding="utf-8") as f:
            await f.write("unsupported content")

    async def asyncTearDown(self):
        """Clean up test directory asynchronously"""
        for file in self.scratch_dir.iterdir():
            file.unlink()
        self.scratch_dir.rmdir()

    async def test_async_load_json_file_in_path(self):
        """Test loading a valid JSON file asynchronously"""
        result = await async_load_json_file_in_path(self.test_json_file)
        self.assertEqual(result, {"key": "value"})

        # Test invalid JSON file
        with self.assertRaises(ValueError):
            await async_load_json_file_in_path(self.test_invalid_json_file)

        # Test missing file
        with self.assertRaises(FileNotFoundError):
            await async_load_json_file_in_path(self.test_missing_file)

        # Test invalid file_path type
        with self.assertRaises(TypeError):
            await async_load_json_file_in_path(123)

    async def test_async_load_file_in_path(self):
        """Test loading a valid JSON file through async_load_file_in_path"""
        result = await async_load_file_in_path(self.test_json_file)
        self.assertEqual(result, {"key": "value"})

        # Test missing file
        with self.assertRaises(FileNotFoundError):
            await async_load_file_in_path(self.test_missing_file)

        # Test unsupported file type
        with self.assertRaises(ValueError):
            await async_load_file_in_path(self.test_unsupported_file)

        # Test invalid file_path type
        with self.assertRaises(TypeError):
            await async_load_file_in_path(123)

    async def test_async_load_multiple_files(self):
        """Test loading multiple JSON files asynchronously"""
        file_paths = [self.test_json_file, self.test_json_file]  # Using same valid file twice
        result = await async_load_multiple_files(file_paths)
        self.assertEqual(result, [{"key": "value"}, {"key": "value"}])

        # Test empty list
        result = await async_load_multiple_files([])
        self.assertEqual(result, [])

        # Test with a missing file
        with self.assertRaises(FileNotFoundError):
            await async_load_multiple_files([self.test_json_file, self.test_missing_file])

        # Test invalid file_paths type
        with self.assertRaises(TypeError):
            await async_load_multiple_files("not_a_list")

        with self.assertRaises(TypeError):
            await async_load_multiple_files([self.test_json_file, 123])




import json
import unittest
from pathlib import Path

import yaml

from utils.common.file_util import (file_exists_in_path, load_file_in_path,
                                    load_json_file_in_path,
                                    load_multiline_sql_file_in_path,
                                    load_multiple_files_in_path,
                                    load_yaml_file_in_path)


class TestFileUtils(unittest.TestCase):

    def setUp(self):
        self.scratch_dir = Path("unittests/scratch_dir")
        self.scratch_dir.mkdir(parents=True, exist_ok=True)
        self.test_json_file = self.scratch_dir / "test.json"
        self.test_yaml_file = self.scratch_dir / "test.yaml"
        self.test_sql_file = self.scratch_dir / "test.sql"
        self.test_invalid_file = self.scratch_dir / "test.txt"

    def tearDown(self):
        for file in self.scratch_dir.iterdir():
            file.unlink()
        self.scratch_dir.rmdir()

    def test_file_exists_in_path(self):
        """Test checking file existence"""
        with open(self.test_json_file, "w") as f:
            f.write("{}")
        self.assertTrue(file_exists_in_path(self.test_json_file))
        self.assertFalse(file_exists_in_path(self.scratch_dir / "missing.json"))
        with self.assertRaises(TypeError):
            file_exists_in_path(123)

    def test_load_json_file_in_path(self):
        """Test loading a JSON file"""
        data = {"key": "value"}
        with open(self.test_json_file, "w") as f:
            json.dump(data, f)
        self.assertEqual(load_json_file_in_path(self.test_json_file), data)
        with self.assertRaises(FileNotFoundError):
            load_json_file_in_path(self.scratch_dir / "missing.json")
        with open(self.test_json_file, "w") as f:
            f.write("invalid json")
        with self.assertRaises(ValueError):
            load_json_file_in_path(self.test_json_file)
        with self.assertRaises(TypeError):
            load_json_file_in_path(123)

    def test_load_yaml_file_in_path(self):
        """Test loading a YAML file"""
        data = {"key": "value"}
        with open(self.test_yaml_file, "w") as f:
            yaml.dump(data, f)
        self.assertEqual(load_yaml_file_in_path(self.test_yaml_file), data)
        with self.assertRaises(FileNotFoundError):
            load_yaml_file_in_path(self.scratch_dir / "missing.yaml")
        with open(self.test_yaml_file, "w") as f:
            f.write("invalid: yaml: : :")
        with self.assertRaises(ValueError):
            load_yaml_file_in_path(self.test_yaml_file)
        with self.assertRaises(TypeError):
            load_yaml_file_in_path(123)

    def test_load_file_in_path(self):
        """Test loading a file based on type"""
        data = {"key": "value"}
        with open(self.test_json_file, "w") as f:
            json.dump(data, f)
        with open(self.test_yaml_file, "w") as f:
            yaml.dump(data, f)
        self.assertEqual(load_file_in_path(self.test_json_file), data)
        self.assertEqual(load_file_in_path(self.test_yaml_file), data)
        with self.assertRaises(FileNotFoundError):
            load_file_in_path(self.scratch_dir / "missing.json")
        with open(self.test_invalid_file, "w") as f:
            f.write("invalid content")
        with self.assertRaises(ValueError):
            load_file_in_path(self.test_invalid_file)
        with self.assertRaises(TypeError):
            load_file_in_path(123)

    def test_load_multiple_files_in_path(self):
        """Test loading multiple files"""
        data = {"key": "value"}
        with open(self.test_json_file, "w") as f:
            json.dump(data, f)
        with open(self.test_yaml_file, "w") as f:
            yaml.dump(data, f)
        file_paths = [self.test_json_file, self.test_yaml_file]
        self.assertEqual(load_multiple_files_in_path(file_paths), [data, data])
        self.assertEqual(load_multiple_files_in_path([]), [])
        with self.assertRaises(FileNotFoundError):
            load_multiple_files_in_path([self.test_json_file, self.scratch_dir / "missing.yaml"])
        with self.assertRaises(TypeError):
            load_multiple_files_in_path("not_a_list")

    def test_load_multiline_sql_file_in_path(self):
        """Test parsing multiline SQL file with comments"""
        sql_content = """
        -- This is a comment
        SELECT * FROM users; 

        /* Multi-line 
           comment */
        INSERT INTO users (id, name) VALUES (1, 'Alice');

        DELETE FROM users WHERE id = 2;
        """

        expected_statements = [
            "SELECT * FROM users",
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "DELETE FROM users WHERE id = 2"
        ]

        self.assertEqual(load_multiline_sql_file_in_path(sql_content), expected_statements)

        # Test empty SQL
        self.assertEqual(load_multiline_sql_file_in_path(""), [])

        # Test only comments
        self.assertEqual(load_multiline_sql_file_in_path("-- comment\n/* multi-line comment */"), [])

    def test_load_file_in_path_unsupported_file(self):
        """Test unsupported file types in load_file_in_path"""
        with open(self.test_invalid_file, "w") as f:
            f.write("Invalid file format")
        with self.assertRaises(ValueError):
            load_file_in_path(self.test_invalid_file)

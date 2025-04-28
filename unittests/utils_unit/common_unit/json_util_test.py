import json
import unittest

from utils.common.json_util import dump_json_data


class TestDumpJsonData(unittest.TestCase):

    def test_valid_json_dump(self):
        # Test dumping dictionary
        data = {"key": "value"}
        expected = json.dumps(data, indent=2)
        self.assertEqual(dump_json_data(data, 2), expected)

        # Test dumping list
        data = [1, 2, 3]
        expected = json.dumps(data, indent=4)
        self.assertEqual(dump_json_data(data, 4), expected)

        # Test dumping nested data
        data = {"nested": {"list": [1, 2]}}
        expected = json.dumps(data, indent=3)
        self.assertEqual(dump_json_data(data, 3), expected)

    def test_invalid_indent_type(self):
        # Expect TypeError for non-integer indent
        with self.assertRaises(TypeError):
            dump_json_data({"key": "value"}, "4")  # Passing a string instead of int

        with self.assertRaises(TypeError):
            dump_json_data({"key": "value"}, None)  # Passing None

        with self.assertRaises(TypeError):
            dump_json_data({"key": "value"}, 3.5)  # Passing float instead of int

    def test_non_serializable_data(self):
        # Expect TypeError for non-serializable data
        with self.assertRaises(TypeError):
            dump_json_data(set([1, 2, 3]), 2)  # JSON does not support set

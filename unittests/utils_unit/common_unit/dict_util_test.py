import unittest

from utils.common.dict_util import merge_dicts


class TestDictUtils(unittest.TestCase):

    def test_merge_dicts(self):
        # Test merging multiple dictionaries
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 3, "c": 4}
        dict3 = {"d": 5}

        result = merge_dicts(dict1, dict2, dict3)
        expected = {"a": 1, "b": 3, "c": 4, "d": 5}
        self.assertEqual(result, expected)

        # Test with no arguments
        result = merge_dicts()
        self.assertEqual(result, {})

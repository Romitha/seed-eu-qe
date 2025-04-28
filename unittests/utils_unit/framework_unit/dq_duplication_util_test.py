import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

from utils.framework.data_quality_utils.duplication_util import (
    check_src_column_name_duplicates,
    check_src_row_duplicates,
    check_trg_column_name_duplicates,
    check_trg_latest_row_duplicates,
)


class TestDuplicationUtil(unittest.TestCase):

    def setUp(self):
        self.engine = MagicMock()
        self.schema_name = "public"
        self.table_name = "users"
        self.expected_columns = ["id", "name", "email", "created_at"]
        self.unique_columns = ["id", "email"]
        self.sys_insert_column = "created_at"

    # === check_src_column_name_duplicates ===

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_src_column_name_duplicates_found(self, mock_read_sql):
        mock_read_sql.return_value = [{"column_name": "dup_col", "duplicate_count": 2}]
        result = check_src_column_name_duplicates(self.engine, self.schema_name, self.table_name)
        self.assertFalse(result['status'])
        self.assertIn("dup_col", result['test_details']['duplicate_columns'])

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_src_column_name_duplicates_not_found(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_src_column_name_duplicates(self.engine, self.schema_name, self.table_name)
        self.assertTrue(result['status'])
        self.assertEqual(result['test_details']['duplicate_columns'], [])

    # === check_trg_column_name_duplicates ===

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_trg_column_name_duplicates_found(self, mock_read_sql):
        mock_read_sql.return_value = [{"column_name": "email", "duplicate_count": 2}]
        result = check_trg_column_name_duplicates(self.engine, self.schema_name, self.table_name)
        self.assertFalse(result['status'])

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_trg_column_name_duplicates_not_found(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_trg_column_name_duplicates(self.engine, self.schema_name, self.table_name)
        self.assertTrue(result['status'])

    # === check_src_row_duplicates ===

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_src_row_duplicates_with_unique_columns(self, mock_read_sql):
        mock_read_sql.return_value = [{"id": 1, "email": "test@example.com", "duplicate_count": 2}]
        result = check_src_row_duplicates(self.engine, self.schema_name, self.table_name,
                                          self.expected_columns, self.unique_columns)
        self.assertFalse(result['status'])
        self.assertEqual(len(result['test_details']['duplicate_rows']), 1)

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_src_row_duplicates_without_unique_columns(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_src_row_duplicates(self.engine, self.schema_name, self.table_name,
                                          self.expected_columns, [])
        self.assertTrue(result['status'])

    # === check_trg_latest_row_duplicates ===

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_latest_row_duplicates_found(self, mock_read_sql):
        mock_read_sql.return_value = [{"id": 1, "email": "test@example.com", "duplicate_count": 2}]
        result = check_trg_latest_row_duplicates(
            self.engine, self.schema_name, self.table_name, self.expected_columns,
            self.unique_columns, self.sys_insert_column)
        self.assertFalse(result['status'])

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_latest_row_duplicates_not_found(self, mock_read_sql):
        """Test when no duplicate rows exist in the latest batch"""
        mock_read_sql.return_value = []
        result = check_trg_latest_row_duplicates(self.engine, self.schema_name, self.table_name,
                                                 self.expected_columns, self.unique_columns, self.sys_insert_column)
        self.assertTrue(result['status'])

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_latest_row_duplicates_empty_table(self, mock_read_sql):
        """Test when the table is empty and no rows exist"""
        mock_read_sql.return_value = []
        result = check_trg_latest_row_duplicates(self.engine, self.schema_name, self.table_name,
                                                 self.expected_columns, self.unique_columns, self.sys_insert_column)
        self.assertTrue(result['status'])

    @patch("utils.framework.data_quality_utils.duplication_util.read_sql_query")
    def test_check_latest_row_duplicates_column_with_spaces(self, mock_read_sql):
        mock_read_sql.return_value = [{
            "id": 1,
            "email": "test@example.com",
            "created_at": "2024-01-01",
            "duplicate_count": 2
        }]
        columns_with_spaces = ["id ", "email ", "created_at "]
        result = check_trg_latest_row_duplicates(
            self.engine, self.schema_name, self.table_name,
            columns_with_spaces, None, "created_at "
        )
        self.assertFalse(result["status"])
        self.assertIn("Duplicate rows detected", result["test_details"]["message"])

    def test_column_regex_failure(self):
        # Simulates what happens if a column name is just spaces (re.match().group() fails)
        with self.assertRaises(AttributeError):
            check_src_row_duplicates(self.engine, self.schema_name, self.table_name,
                                     ["   "], ["id"])

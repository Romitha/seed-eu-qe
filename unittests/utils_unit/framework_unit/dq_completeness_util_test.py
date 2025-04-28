import unittest
from unittest.mock import patch, MagicMock

from utils.framework.data_quality_utils.completeness_util import (
    check_missing_column, check_blank_rows, check_unexpected_nulls,
    check_src_missing_column, check_src_blank_rows, check_src_unexpected_nulls,
    validate_external_table_schema, validate_internal_table_schema,
)


class TestCompletenessUtil(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.schema = "public"
        self.table = "users"
        self.columns = ["id", "name", "email"]
        self.test_cols_for_nulls = ["name", "email"]

    # === check_missing_column ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_missing_column_no_missing(self, mock_read_sql):
        mock_read_sql.return_value = [{"column_name": col} for col in self.columns]
        result = check_missing_column(self.client, self.schema, self.table, self.columns)
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_missing_column_some_missing(self, mock_read_sql):
        mock_read_sql.return_value = [{"column_name": "id"}]
        result = check_missing_column(self.client, self.schema, self.table, self.columns)
        self.assertFalse(result["status"])
        self.assertIn("name", result["test_details"]["missing_columns"])

    # === check_blank_rows ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_blank_rows_with_blank(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"column_name": col} for col in self.columns],
            [{"blank_row_count": 2}]
        ]
        result = check_blank_rows(self.client, self.schema, self.table)
        self.assertFalse(result["status"])
        self.assertEqual(result["test_details"]["blank_row_count"], 2)

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_blank_rows_no_blank(self, mock_read_sql):
        """Test case where no blank rows exist."""
        mock_read_sql.side_effect = [
            [{"column_name": col} for col in self.columns],
            [{"blank_row_count": 0}]
        ]
        result = check_blank_rows(self.client, self.schema, self.table)
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_blank_rows_table_not_found(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_blank_rows(self.client, self.schema, self.table)
        self.assertFalse(result["status"])
        self.assertIsNone(result["test_details"]["blank_row_count"])

    # === check_unexpected_nulls ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_unexpected_nulls_with_nulls(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"null_count": 2}],
            [{"null_count": 0}]
        ]
        result = check_unexpected_nulls(self.client, self.schema, self.table, self.test_cols_for_nulls)
        self.assertFalse(result["status"])
        self.assertIn("name", result["test_details"]["columns_with_nulls"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_unexpected_nulls_all_allowed(self, _):
        result = check_unexpected_nulls(self.client, self.schema, self.table, [])
        self.assertTrue(result["status"])
        self.assertEqual(result["test_details"]["columns_with_nulls"], [])

    # === check_src_missing_column ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_src_missing_column_some_missing(self, mock_read_sql):
        mock_read_sql.return_value = [{"column_name": "id"}]
        result = check_src_missing_column(self.client, self.schema, self.table, self.columns)
        self.assertFalse(result["status"])
        self.assertIn("name", result["test_details"]["missing_columns"])

    # === check_src_blank_rows ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_src_blank_rows_none_blank(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"column_name": col} for col in self.columns],
            [{"blank_row_count": 0}]
        ]
        result = check_src_blank_rows(self.client, self.schema, self.table)
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_src_blank_rows_table_not_found(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_src_blank_rows(self.client, self.schema, self.table)
        self.assertFalse(result["status"])
        self.assertIn("not found", result["test_details"]["message"])

    # === check_src_unexpected_nulls ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_src_unexpected_nulls_with_nulls(self, mock_read_sql):
        mock_read_sql.side_effect = [[{"null_count": 1}], [{"null_count": 0}]]
        result = check_src_unexpected_nulls(self.client, self.schema, self.table, self.test_cols_for_nulls)
        self.assertFalse(result["status"])
        self.assertIn("name", result["test_details"]["columns_with_nulls"])

    # === validate_external_table_schema ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_validate_external_table_schema(self, mock_read_sql):
        mock_read_sql.return_value = [
            {"column_name": "id", "data_type": "int"},
            {"column_name": "name", "data_type": "varchar(50)"}
        ]
        expected_schema = {"id": "int", "name": "varchar(50)"}
        result = validate_external_table_schema(self.client, self.schema, self.table, expected_schema)
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_validate_external_table_schema_with_mismatch(self, mock_read_sql):
        mock_read_sql.return_value = [
            {"column_name": "id", "data_type": "int"},
            {"column_name": "name", "data_type": "text"}
        ]
        expected_schema = {"id": "int", "name": "varchar(50)"}
        result = validate_external_table_schema(self.client, self.schema, self.table, expected_schema)
        self.assertFalse(result["status"])

    # === validate_internal_table_schema ===

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_validate_internal_table_schema(self, mock_read_sql):
        mock_read_sql.return_value = [
            {"column_name": "id", "data_type": "integer", "character_maximum_length": None,
             "numeric_precision": None, "numeric_scale": None},
            {"column_name": "name", "data_type": "character varying", "character_maximum_length": 50,
             "numeric_precision": None, "numeric_scale": None}
        ]
        expected_schema = {"id": "integer", "name": "varchar(50)"}
        result = validate_internal_table_schema(self.client, self.schema, self.table, expected_schema)
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_validate_internal_table_schema_with_mismatch(self, mock_read_sql):
        mock_read_sql.return_value = [
            {"column_name": "id", "data_type": "integer", "character_maximum_length": None,
             "numeric_precision": None, "numeric_scale": None},
            {"column_name": "name", "data_type": "text", "character_maximum_length": None,
             "numeric_precision": None, "numeric_scale": None}
        ]
        expected_schema = {"id": "integer", "name": "varchar(50)"}
        result = validate_internal_table_schema(self.client, self.schema, self.table, expected_schema)
        self.assertFalse(result["status"])

    @patch("utils.framework.data_quality_utils.completeness_util.read_sql_query")
    def test_check_blank_rows_missing_blank_row_key(self, mock_read_sql):
        """Simulate blank row query result with missing key"""
        mock_read_sql.side_effect = [
            [{"column_name": col} for col in self.columns],
            [{}],  # result returned but missing "blank_row_count"
        ]
        result = check_blank_rows(self.client, self.schema, self.table)
        self.assertEqual(result["test_details"]["blank_row_count"], 0)

    def test_check_src_unexpected_nulls_empty_column_list(self):
        """Test when no columns are given (all nullable)"""
        from utils.framework.data_quality_utils.completeness_util import check_src_unexpected_nulls
        result = check_src_unexpected_nulls(self.client, self.schema, self.table, [])
        self.assertTrue(result["status"])
        self.assertIn("are allowed to be NULL", result["test_details"]["message"])

    def test_check_unexpected_nulls_empty_column_list(self):
        """Internal table: no columns to check for nulls"""
        result = check_unexpected_nulls(self.client, self.schema, self.table, [])
        self.assertTrue(result["status"])
        self.assertIn("allowed to be NULL", result["test_details"]["message"])

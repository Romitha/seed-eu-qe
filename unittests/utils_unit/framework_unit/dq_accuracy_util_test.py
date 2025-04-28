import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from utils.framework.data_quality_utils.accuracy_util import (
    check_numeric_precision_for_column
)


class TestAccuracyUtil(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.schema = "public"
        self.table = "orders"
        self.column = "amount"
        self.settings = {"num_precision": 10, "numeric_scale": 2}

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_column_precision_and_scale_match(self, mock_read_sql):
        mock_read_sql.return_value = [{"numeric_precision": 10, "numeric_scale": 2}]
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertTrue(result["status"])
        self.assertIn("match expected values", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_column_precision_mismatch_only(self, mock_read_sql):
        mock_read_sql.return_value = [{"numeric_precision": 8, "numeric_scale": 2}]
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertFalse(result["status"])
        self.assertIn("Precision mismatch", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_column_scale_mismatch_only(self, mock_read_sql):
        mock_read_sql.return_value = [{"numeric_precision": 10, "numeric_scale": 0}]
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertFalse(result["status"])
        self.assertIn("Scale mismatch", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_column_precision_and_scale_mismatch(self, mock_read_sql):
        mock_read_sql.return_value = [{"numeric_precision": 5, "numeric_scale": 1}]
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertFalse(result["status"])
        self.assertIn("Precision mismatch", result["test_details"]["message"])
        self.assertIn("Scale mismatch", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_column_not_found(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertFalse(result["status"])
        self.assertIn("is not found", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.accuracy_util.read_sql_query")
    def test_database_error(self, mock_read_sql):
        mock_read_sql.side_effect = SQLAlchemyError("DB failure")
        result = check_numeric_precision_for_column(
            self.client, self.schema, self.table, self.column, self.settings
        )
        self.assertFalse(result["status"])
        self.assertIn("Database error", result["test_details"]["message"])

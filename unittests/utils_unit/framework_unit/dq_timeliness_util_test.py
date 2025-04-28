import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

from utils.framework.data_quality_utils.timeliness_util import \
    check_timeliness_in_latest_batch


class TestTimelinessUtils(unittest.TestCase):

    def setUp(self):
        self.engine = MagicMock()
        self.schema_name = "test_schema"
        self.table_name = "test_table"
        self.sys_dt_col = "created_at"
        self.expected_within_hours = 24

    @patch("utils.framework.data_quality_utils.timeliness_util.read_sql_query")
    def test_check_timeliness_in_latest_batch_success(self, mock_read_sql):
        """Latest insert is within the expected timeframe"""
        mock_read_sql.side_effect = [
            [{"latest_insert": "2024-10-08 12:00:00"}],
            [{"hours_difference": 5}]
        ]
        result = check_timeliness_in_latest_batch(
            self.engine, self.schema_name, self.table_name,
            self.sys_dt_col, self.expected_within_hours
        )
        self.assertTrue(result["status"])
        self.assertIn("Insert timeliness", result["test_details"]["message"])
        self.assertEqual(result["test_details"]["hours_difference"], 5)

    @patch("utils.framework.data_quality_utils.timeliness_util.read_sql_query")
    def test_check_timeliness_in_latest_batch_too_old(self, mock_read_sql):
        """Latest insert is too old (outside allowed window)"""
        mock_read_sql.side_effect = [
            [{"latest_insert": "2024-10-08 12:00:00"}],
            [{"hours_difference": 100}]
        ]
        result = check_timeliness_in_latest_batch(
            self.engine, self.schema_name, self.table_name,
            self.sys_dt_col, self.expected_within_hours
        )
        self.assertFalse(result["status"])
        self.assertIn("exceeding the expected", result["test_details"]["message"])
        self.assertEqual(result["test_details"]["hours_difference"], 100)

    @patch("utils.framework.data_quality_utils.timeliness_util.read_sql_query")
    def test_check_timeliness_in_latest_batch_no_data(self, mock_read_sql):
        """No rows in the table — no latest insert"""
        mock_read_sql.return_value = [{"latest_insert": None}]
        result = check_timeliness_in_latest_batch(
            self.engine, self.schema_name, self.table_name,
            self.sys_dt_col, self.expected_within_hours
        )
        self.assertFalse(result["status"])
        self.assertIsNone(result["test_details"]["latest_insert_time"])
        self.assertIn("No data found", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.timeliness_util.read_sql_query")
    def test_check_timeliness_in_latest_batch_sqlalchemy_error(self, mock_read_sql):
        """Simulate SQLAlchemyError from DB"""
        mock_read_sql.side_effect = SQLAlchemyError("DB error")
        result = check_timeliness_in_latest_batch(
            self.engine, self.schema_name, self.table_name,
            self.sys_dt_col, self.expected_within_hours
        )
        self.assertFalse(result["status"])
        self.assertIn("Error during insert timeliness check", result["test_details"]["message"])
        self.assertIsNone(result["test_details"]["latest_insert_time"])

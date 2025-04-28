import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from utils.framework.data_quality_utils.consistency_util import (
    check_column_count_consistency,
    check_row_count_consistency,
    check_col_and_row_data_consistency
)


class TestConsistencyUtil(unittest.TestCase):

    def setUp(self):
        self.engine = MagicMock()
        self.src_schema = "source_schema"
        self.src_table = "source_table"
        self.trg_schema = "target_schema"
        self.trg_table = "target_table"
        self.sys_cols_count = 2
        self.scd_cols_count = 1
        self.unique_columns = ["id"]
        self.mapped_cols = ["id", "name", "dob"]
        self.cols_cast = ["dob"]

    # === check_column_count_consistency ===

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_column_count_consistency_match_internal(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"count": 5}],  # source
            [{"count": 8}],  # target (5+2+1 == 8)
        ]
        result = check_column_count_consistency(
            self.engine, True, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, self.sys_cols_count, self.scd_cols_count
        )
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_column_count_consistency_mismatch_external(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"count": 3}],  # source (3+2+1 = 6)
            [{"count": 5}],  # target
        ]
        result = check_column_count_consistency(
            self.engine, False, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, self.sys_cols_count, self.scd_cols_count
        )
        self.assertFalse(result["status"])
        self.assertIn("NOT matched", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_column_count_consistency_missing_data(self, mock_read_sql):
        mock_read_sql.side_effect = [[], []]
        result = check_column_count_consistency(
            self.engine, False, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, self.sys_cols_count, self.scd_cols_count
        )
        self.assertFalse(result["status"])
        self.assertIsNone(result["test_details"]["source_count"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_column_count_consistency_error(self, mock_read_sql):
        mock_read_sql.side_effect = Exception("DB error")
        result = check_column_count_consistency(
            self.engine, False, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, self.sys_cols_count, self.scd_cols_count
        )
        self.assertFalse(result["status"])
        self.assertIn("Error during column count", result["test_details"]["message"])

    # === check_row_count_consistency ===

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_row_count_consistency_match(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"row_count": 100}],  # source
            [{"row_count": 100}],  # target
        ]
        result = check_row_count_consistency(
            self.engine, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, synth_data=False
        )
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_row_count_consistency_mismatch(self, mock_read_sql):
        mock_read_sql.side_effect = [
            [{"row_count": 100}],  # source
            [{"row_count": 90}],   # target
        ]
        result = check_row_count_consistency(
            self.engine, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, synth_data=False
        )
        self.assertFalse(result["status"])
        self.assertIn("NOT matched", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_row_count_consistency_missing_data(self, mock_read_sql):
        mock_read_sql.side_effect = [[], []]
        result = check_row_count_consistency(
            self.engine, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, synth_data=False
        )
        self.assertFalse(result["status"])
        self.assertIsNone(result["test_details"]["source_count"])

    def test_row_count_consistency_synthetic_data(self):
        result = check_row_count_consistency(
            self.engine, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, synth_data=True
        )
        self.assertEqual(result["status"], "Skipped")

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_row_count_consistency_error(self, mock_read_sql):
        mock_read_sql.side_effect = Exception("Failure")
        result = check_row_count_consistency(
            self.engine, self.src_schema, self.src_table,
            self.trg_schema, self.trg_table, synth_data=False
        )
        self.assertFalse(result["status"])
        self.assertIn("Error during row count", result["test_details"]["message"])

    # === check_col_and_row_data_consistency ===

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_col_and_row_data_consistency_match(self, mock_read_sql):
        mock_read_sql.return_value = []
        result = check_col_and_row_data_consistency(
            self.engine, self.src_schema, self.src_table, self.trg_schema, self.trg_table,
            self.unique_columns, self.mapped_cols, self.cols_cast, scd_enabled=False
        )
        self.assertTrue(result["status"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_col_and_row_data_consistency_missing_rows(self, mock_read_sql):
        mock_read_sql.return_value = [{"id": 1, "name": "Missing"}]
        result = check_col_and_row_data_consistency(
            self.engine, self.src_schema, self.src_table, self.trg_schema, self.trg_table,
            self.unique_columns, self.mapped_cols, self.cols_cast, scd_enabled=False
        )
        self.assertFalse(result["status"])
        self.assertIn("Missing", str(result["test_details"]["missing_rows"]))

    def test_col_and_row_data_consistency_scd_enabled(self):
        result = check_col_and_row_data_consistency(
            self.engine, self.src_schema, self.src_table, self.trg_schema, self.trg_table,
            self.unique_columns, self.mapped_cols, self.cols_cast, scd_enabled=True
        )
        self.assertEqual(result, "Skipping row-level comparison for SCD table")

    def test_col_and_row_data_consistency_empty_columns(self):
        result = check_col_and_row_data_consistency(
            self.engine, self.src_schema, self.src_table, self.trg_schema, self.trg_table,
            self.unique_columns, [], self.cols_cast, scd_enabled=False
        )
        self.assertFalse(result["status"])
        self.assertIn("clean_mapped_cols cannot be empty", result["test_details"]["message"])

    @patch("utils.framework.data_quality_utils.consistency_util.read_sql_query")
    def test_col_and_row_data_consistency_error(self, mock_read_sql):
        mock_read_sql.side_effect = Exception("Failure")
        result = check_col_and_row_data_consistency(
            self.engine, self.src_schema, self.src_table, self.trg_schema, self.trg_table,
            self.unique_columns, self.mapped_cols, self.cols_cast, scd_enabled=False
        )
        self.assertFalse(result["status"])
        self.assertIn("Error during column and row data", result["test_details"]["message"])

import unittest
from datetime import datetime
from unittest.mock import patch

from faker import Faker
from sqlalchemy import (Column, DateTime, Float, Integer, MetaData, String,
                        Table, create_engine)

from utils.common.synthetic_data_util import (
    delete_synthetic_data, ensure_src_sys_cd_column, generate_synthetic_data,
    generate_table_schema_from_columns, insert_synthetic_data)

fake = Faker()


class TestSyntheticDataUtil(unittest.TestCase):

    def test_generate_table_schema_from_columns(self):
        """Test schema generation from SQL column definitions"""
        columns = ["id INT", "name VARCHAR(50)", "price NUMERIC(30, 20)", "created_at TIMESTAMP"]
        expected_schema = {
            "id": ["int", 10],
            "name": ["str", 50],
            "price": ["float", 10.1],
            "created_at": ["timestamp", None],
        }
        self.assertEqual(generate_table_schema_from_columns(columns), expected_schema)

    def test_ensure_src_sys_cd_column(self):
        """Test adding required metadata columns if missing"""
        schema = {"id": ["int", 10], "name": ["str", 50]}
        updated_schema = ensure_src_sys_cd_column(schema)
        expected_schema = {
            "id": ["int", 10],
            "name": ["str", 50],
            "src_sys_cd": ["str", 10],
            "insrt_dttm": ["timestamp", None],
            "updt_dttm": ["timestamp", None],
        }
        self.assertEqual(updated_schema, expected_schema)

    def test_generate_synthetic_data(self):
        """Test synthetic data generation with different data types"""
        schema = {
            "id": ["int", 10],
            "name": ["str", 50],
            "price": ["float", 10.1],
            "created_at": ["timestamp", None],
        }
        data = generate_synthetic_data(schema, 5)

        self.assertEqual(len(data), 5)
        for row in data:
            self.assertIn("src_sys_cd", row)
            self.assertEqual(row["src_sys_cd"], "XYZ")
            self.assertIsInstance(row["id"], int)
            self.assertIsInstance(row["name"], str)
            self.assertIsInstance(row["price"], float)
            self.assertIsInstance(row["created_at"], datetime)

    def setUp(self):
        """Setup a temporary in-memory SQLite database for testing"""
        self.engine = create_engine("sqlite:///:memory:")  # SQLite does not support schema_name
        self.metadata = MetaData()
        self.table_name = "test_table"

        self.test_table = Table(
            self.table_name,
            self.metadata,
            Column("id", Integer),
            Column("name", String(50)),
            Column("price", Float),
            Column("src_sys_cd", String(10)),
            Column("insrt_dttm", DateTime),
            Column("updt_dttm", DateTime),
        )
        self.metadata.create_all(self.engine)

    def test_insert_synthetic_data(self):
        """Test inserting synthetic data into the database"""
        schema = generate_table_schema_from_columns(["id INT", "name VARCHAR(50)", "price NUMERIC(10,2)"])
        synthetic_data = generate_synthetic_data(schema, 3)

        insert_synthetic_data(self.engine, None, self.table_name, synthetic_data)  # No schema_name for SQLite

        with self.engine.connect() as conn:
            result = conn.execute(self.test_table.select()).fetchall()

        self.assertEqual(len(result), 3)

    def test_delete_synthetic_data(self):
        """Test deleting synthetic data from the database"""
        schema = generate_table_schema_from_columns(["id INT", "name VARCHAR(50)", "price NUMERIC(10,2)"])
        synthetic_data = generate_synthetic_data(schema, 5)

        insert_synthetic_data(self.engine, None, self.table_name, synthetic_data)  # No schema_name for SQLite
        delete_synthetic_data(self.engine, None, self.table_name)  # No schema_name for SQLite

        with self.engine.connect() as conn:
            result = conn.execute(self.test_table.select()).fetchall()

        self.assertEqual(len(result), 0)

    def test_generate_synthetic_data_empty_schema(self):
        """Test generating synthetic data with an empty schema"""
        empty_schema = {}
        data = generate_synthetic_data(empty_schema, 5)

        # Ensure metadata columns are still present
        self.assertEqual(len(data), 5)
        self.assertIn("src_sys_cd", data[0])
        self.assertIn("insrt_dttm", data[0])
        self.assertIn("updt_dttm", data[0])

    @patch("utils.common.synthetic_data_util.datetime")
    def test_generate_synthetic_data_timestamps(self, mock_datetime):
        """Test that timestamps are generated correctly"""
        mock_now = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now

        schema = {"created_at": ["timestamp", None]}
        data = generate_synthetic_data(schema, 1)
        self.assertEqual(data[0]["created_at"], mock_now)

    @patch("utils.common.synthetic_data_util.secrets.choice")
    def test_generate_synthetic_data_random_int(self, mock_choice):
        """Test secure random integer generation"""
        mock_choice.return_value = 42
        schema = {"id": ["int", 10]}
        data = generate_synthetic_data(schema, 1)
        self.assertEqual(data[0]["id"], 42)

    def test_generate_synthetic_data_empty_date_fields(self):
        """Test that columns ending with '_dt' remain empty"""
        schema = {"event_dt": ["str", 20]}
        data = generate_synthetic_data(schema, 1)
        self.assertEqual(data[0]["event_dt"], "")

    def tearDown(self):
        """Drop all tables and clean up the in-memory database"""
        self.metadata.drop_all(self.engine)

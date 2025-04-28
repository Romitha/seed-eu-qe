import unittest

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from utils.common.sqlalchemy_util import (create_sqlalchemy_url,
                                          read_sql_query, run_sql_query)


class TestSqlAlchemyUtils(unittest.TestCase):

    def setUp(self):
        """
        Sets up an in-memory SQLite database for testing
        """
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.connect() as conn:
            conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO test_table (name) VALUES ('Alice'), ('Bob')")

    def test_create_sqlalchemy_url_redshift(self):
        """Tests Redshift URL creation"""
        config = {
            "redshift_host": "example.com",
            "redshift_database": "mydb",
            "redshift_username": "user",
            "redshift_password": "pass"
        }
        url = create_sqlalchemy_url("aws_redshift", config)
        self.assertIsInstance(url, URL)
        self.assertIn("redshift+redshift_connector", str(url))

    def test_create_sqlalchemy_url_invalid(self):
        """Tests invalid database name handling"""
        with self.assertRaises(ValueError):
            create_sqlalchemy_url("invalid_db", {})

    def test_read_sql_query(self):
        """Tests reading data from the database"""
        query = "SELECT * FROM test_table"
        result = read_sql_query(self.engine, query)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['name'], 'Alice')
        self.assertEqual(result[1]['name'], 'Bob')

    def test_read_sql_query_failure(self):
        """Tests query failure handling"""
        with self.assertRaises(RuntimeError):
            read_sql_query(self.engine, "SELECT * FROM non_existent_table")

    def test_run_sql_query(self):
        """Tests running an SQL command"""
        run_sql_query(self.engine, "INSERT INTO test_table (name) VALUES ('Charlie')")
        query = "SELECT * FROM test_table WHERE name = 'Charlie'"
        result = read_sql_query(self.engine, query)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Charlie')

    def test_run_sql_query_failure(self):
        """Tests running an invalid SQL command"""
        with self.assertRaises(RuntimeError):
            run_sql_query(self.engine, "INSERT INTO non_existent_table (name) VALUES ('Charlie')")

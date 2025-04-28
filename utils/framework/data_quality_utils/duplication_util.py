import re

from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_src_column_name_duplicates(engine, schema_name, table_name):
    """
    Checks for duplicate column names in an external table

    Args:
        engine (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Returns:
        dict: Dictionary containing status and details about duplicate column names
    """

    query = f"""
        SELECT columnname AS column_name, COUNT(*) AS duplicate_count
        FROM svv_external_columns
        WHERE schemaname = '{schema_name}' 
          AND tablename = '{table_name}'
        GROUP BY columnname
        HAVING COUNT(*) > 1
    """
    duplicates = read_sql_query(engine, query)

    duplicate_columns = [
        row["column_name"] for row in duplicates
    ] if duplicates else []

    status = len(duplicate_columns) == 0
    details = {
        'duplicate_columns': duplicate_columns,
        'message': (
            f"No duplicate column names found in {table_name}"
            if status
            else f"Duplicate column names found: {', '.join(duplicate_columns)}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_src_row_duplicates(engine, schema_name, table_name, expected_columns, unique_columns):
    """
    Checks for duplicate rows in the latest batch based on a timestamp column

    Args:
        engine (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        expected_columns (list): List of expected column names
        unique_columns (list): List of columns that should be unique (if provided)

    Returns:
        dict: Dictionary containing status and details about duplicate rows
    """

    # Extract column names (removing extra spaces)
    expected_columns = [re.match(r'^\S+', col).group() for col in expected_columns]
    cols = unique_columns if unique_columns else expected_columns

    unique_cols_str = ', '.join(cols)

    query = f"""
        WITH all_rows AS (
            SELECT * FROM {schema_name}.{table_name}
        )
        SELECT {unique_cols_str}, COUNT(*) AS duplicate_count
        FROM all_rows
        GROUP BY {unique_cols_str}
        HAVING COUNT(*) > 1;
    """
    duplicates = read_sql_query(engine, query)

    duplicate_rows = [
        {col: row[col] for col in cols} for row in duplicates
    ] if duplicates else []

    status = len(duplicate_rows) == 0
    details = {
        'duplicate_rows': duplicate_rows,
        'message': (
            f"No duplicate rows found in {table_name}"
            if status
            else f"Duplicate rows detected in {table_name}: {len(duplicate_rows)} instances"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_trg_column_name_duplicates(engine, schema_name, table_name):
    """
    Checks for duplicate column names in an internal table

    Args:
        engine (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Returns:
        dict: Dictionary containing status and details about duplicate column names
    """

    query = f"""
        SELECT column_name, COUNT(*) as duplicate_count
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}'
        GROUP BY column_name
        HAVING COUNT(*) > 1
    """
    duplicates = read_sql_query(engine, query)

    duplicate_columns = [
        row["column_name"] for row in duplicates
    ] if duplicates else []

    status = len(duplicate_columns) == 0
    details = {
        'duplicate_columns': duplicate_columns,
        'message': (
            f"No duplicate column names found in {table_name}"
            if status
            else f"Duplicate column names found: {', '.join(duplicate_columns)}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_trg_latest_row_duplicates(
        engine, schema_name, table_name, expected_columns, unique_columns, sys_insert_column):
    """
    Checks for duplicate rows in the latest batch based on a system timestamp column

    Args:
        engine (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        expected_columns (list): List of expected column names
        unique_columns (list): List of columns that should be unique (if provided)
        sys_insert_column (str): Column used to determine the latest batch

    Returns:
        dict: Dictionary containing status and details about duplicate rows in the latest batch
    """

    # Extract clean column names (remove extra spaces)
    expected_columns = [re.match(r'^\S+', col).group() for col in expected_columns]
    cols = unique_columns if unique_columns else expected_columns

    unique_cols_str = ', '.join(cols)
    sys_insert_column = re.match(r'^\S+', sys_insert_column).group()

    query = f"""
        WITH latest_batch AS (
            SELECT * FROM {schema_name}.{table_name}
            WHERE {sys_insert_column} = (
                SELECT MAX({sys_insert_column}) FROM {schema_name}.{table_name}
            )
        )
        SELECT {unique_cols_str}, COUNT(*) AS duplicate_count
        FROM latest_batch
        GROUP BY {unique_cols_str}
        HAVING COUNT(*) > 1
    """
    duplicates = read_sql_query(engine, query)

    duplicate_rows = [
        {col: row[col] for col in cols} for row in duplicates
    ] if duplicates else []

    status = len(duplicate_rows) == 0
    details = {
        'duplicate_rows': duplicate_rows[:5],
        'message': (
            f"No duplicate rows found in the latest batch of {table_name}"
            if status
            else f"Duplicate rows detected in the latest batch of {table_name}: {len(duplicate_rows)} instances"
        )
    }

    return {
        'status': status,
        'test_details': details
    }

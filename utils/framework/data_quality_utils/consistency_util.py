import re

from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_column_count_consistency(
        engine, internal_schema, src_schema, src_table, trg_schema, trg_table, sys_cols_count, scd_cols_count):
    """
    Checks if the column count in the source table matches the target table

    Args:
        engine (Any): Database connection engine
        internal_schema (bool): Flag to indicate if source schema is internal
        src_schema (str): Source schema name
        src_table (str): Source table name
        trg_schema (str): Target schema name
        trg_table (str): Target table name
        sys_cols_count (int): Number of system-generated columns
        scd_cols_count (int): Number of SCD (Slowly Changing Dimension) columns

    Returns:
        dict: Dictionary containing status and details about column count consistency
    """

    table_source = "information_schema.columns" if internal_schema else "svv_external_columns"
    schema_column = "table_schema" if internal_schema else "schemaname"
    table_column = "table_name" if internal_schema else "tablename"

    try:
        # Query for source table column count
        src_query = f"""
            SELECT COUNT(*)
            FROM {table_source}
            WHERE {schema_column} = '{src_schema}'
            AND {table_column} = '{src_table}'
        """
        src_count_result = read_sql_query(engine, src_query)
        src_count = int(src_count_result[0]['count']) + sys_cols_count + scd_cols_count if src_count_result else None

        # Query for target table column count
        trg_query = f"""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = '{trg_schema}' 
            AND table_name = '{trg_table}'
        """
        trg_count_result = read_sql_query(engine, trg_query)
        trg_count = int(trg_count_result[0]['count']) if trg_count_result else None

        # Check for missing data
        if src_count is None or trg_count is None:
            return {
                'status': False,
                'test_details': {
                    'source_count': src_count,
                    'target_count': trg_count,
                    'message':
                        f"Column count comparison failed: "
                        f"Could not retrieve column counts for {src_table} or {trg_table}"
                }
            }

        # Compare column counts
        status = src_count == trg_count
        details = {
            'source_count': src_count,
            'target_count': trg_count,
            'message': (
                f"Source column count {src_count} matched with target column count {trg_count}"
                if status
                else f"Source column count {src_count} NOT matched with target column count {trg_count}"
            )
        }

        return {
            'status': status,
            'test_details': details
        }

    except Exception as e:
        return {
            'status': False,
            'test_details': {
                'message': f"Error during column count consistency check: {str(e)}"
            }
        }


def check_row_count_consistency(engine, src_schema, src_table, trg_schema, trg_table, synth_data, where_clause=None):
    """
    Checks if the row count in the source table matches the target table

    Args:
        engine (Any): Database connection engine
        src_schema (str): Source schema name
        src_table (str): Source table name
        trg_schema (str): Target schema name
        trg_table (str): Target table name
        synth_data (bool): Flag indicating if synthetic data is being used
        where_clause (str, optional): Additional WHERE clause to filter target table rows

    Returns:
        dict: Dictionary containing status and details about row count consistency
    """

    if synth_data:
        return {
            'status': "Skipped",
            'test_details': {
                'message': f'Skipping row count comparison since synthetic data is: {synth_data}'
            }
        }

    try:
        # Query for source table row count
        src_query = f"""
            SELECT COUNT(*) AS row_count
            FROM {src_schema}.{src_table}
        """
        src_count_result = read_sql_query(engine, src_query)
        src_count = int(src_count_result[0]['row_count']) if src_count_result else None

        # Query for target table row count with optional WHERE clause
        trg_query = f"""
            SELECT COUNT(*) AS row_count
            FROM {trg_schema}.{trg_table}
            {where_clause if where_clause else ""}
        """
        trg_count_result = read_sql_query(engine, trg_query)
        trg_count = int(trg_count_result[0]['row_count']) if trg_count_result else None

        # Check for missing data
        if src_count is None or trg_count is None:
            return {
                'status': False,
                'test_details': {
                    'source_count': src_count,
                    'target_count': trg_count,
                    'message':
                        f"Row count comparison failed: Could not retrieve row counts for {src_table} or {trg_table}"
                }
            }

        # Compare row counts
        status = src_count == trg_count
        details = {
            'source_count': src_count,
            'target_count': trg_count,
            'message': (
                f"Source row count {src_count} matched with target row count {trg_count}"
                if status
                else f"Source row count {src_count} NOT matched with target row count {trg_count}"
            )
        }

        return {
            'status': status,
            'test_details': details
        }

    except Exception as e:
        return {
            'status': False,
            'test_details': {
                'message': f"Error during row count consistency check: {str(e)}"
            }
        }


def check_col_and_row_data_consistency(
        engine, src_schema, src_table, trg_schema, trg_table, unique_columns, clean_mapped_cols,cols_needing_cast, scd_enabled
):
    """
    Checks for column and row mismatches using a dynamic LEFT JOIN approach with COALESCE.

    Args:
        engine: Database connection engine
        src_schema (str): Source schema name
        src_table (str): Source table name
        trg_schema (str): Target schema name
        trg_table (str): Target table name
        clean_mapped_cols (list): List of target table column definitions

    Returns:
        dict: Status and test details

    Raises:
        ValueError: If clean_mapped_cols is empty
    """

    try:
        if scd_enabled:
            return 'Skipping row-level comparison for SCD table'

        # Validate that clean_mapped_cols is not empty
        if not clean_mapped_cols:
            raise ValueError("clean_mapped_cols cannot be empty for comparison.")

        # Function to safely reference column names
        def safe_col_name(col):
            return f'"{col}"' if re.match(r"^\d", col) else col

        # Hash calculation for the source table (convert dates)

        src_hash_expr = " || '|' || ".join([
            f"COALESCE(TO_CHAR(TO_TIMESTAMP({safe_col_name(col)}, 'DD/MM/YYYY'), 'YYYY-MM-DD'), '')"
            if col in cols_needing_cast else f"COALESCE({safe_col_name(col)}::VARCHAR, '')"
            for col in clean_mapped_cols
        ])

        # Hash calculation for the target table (keep original date format)
        trg_hash_expr = " || '|' || ".join([
            f"COALESCE(TO_CHAR({safe_col_name(col)}, 'YYYY-MM-DD'), '')"
            if col in cols_needing_cast else f"COALESCE({safe_col_name(col)}::VARCHAR, '')"
            for col in clean_mapped_cols
        ])

        # Construct the SQL query similar to the example
        query = f"""
                WITH src AS (
                  SELECT
                    MD5({src_hash_expr}) AS row_hash,
                    *
                  FROM {src_schema}.{src_table}
                ),
                trg AS (
                  SELECT
                    MD5({trg_hash_expr}) AS row_hash
                  FROM {trg_schema}.{trg_table}
                )
                SELECT src.*
                FROM src
                LEFT JOIN trg ON src.row_hash = trg.row_hash
                WHERE trg.row_hash IS NULL;
                """
        print("Generated Query: ", query)
        missing_rows = read_sql_query(engine, query)

        status = not bool(missing_rows)  # True if no missing rows, False otherwise

        return {
            'status': status,
            'test_details': {
                'missing_rows': missing_rows[:5],
                'message': "No missing rows found" if status else "Missing rows detected in the target table"
            }
        }
    except Exception as e:
        return {
            'status': False,
            'test_details': {
                'message': f"Error during column and row data consistency check: {str(e)}"
            }
        }

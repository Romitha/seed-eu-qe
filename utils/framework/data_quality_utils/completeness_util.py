from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_src_missing_column(client, schema_name, table_name, column_names):
    """
    Check for missing columns in an external table
    """
    query = f"""
        SELECT columnname AS column_name
        FROM svv_external_columns
        WHERE schemaname = '{schema_name}'
        AND tablename = '{table_name}'
    """
    result = read_sql_query(client, query)
    table_columns = [row["column_name"] for row in result] if result else []
    missing_columns = [col for col in column_names if col not in table_columns]

    status = len(missing_columns) == 0
    details = {
        'missing_columns': missing_columns,
        'message': (
            'No missing columns'
            if status
            else f"Missing columns detected: {', '.join(missing_columns)}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_src_blank_rows(client, schema_name, table_name):
    """
    Identify blank rows in an external table where all values are NULL
    """
    column_query = f"""
        SELECT columnname AS column_name
        FROM svv_external_columns
        WHERE schemaname = '{schema_name}'
        AND tablename = '{table_name}'
    """
    result = read_sql_query(client, column_query)
    table_columns = [row["column_name"] for row in result] if result else []

    if not table_columns:
        return {
            'status': False,
            'test_details': {
                'blank_row_count': None,
                'message': f"Table {table_name} not found or has no columns"
            }
        }

    null_condition = " AND ".join([f"{col} IS NULL" for col in table_columns])
    query = f"""
        SELECT COUNT(*) AS blank_row_count
        FROM {schema_name}.{table_name}
        WHERE {null_condition}
    """
    result = read_sql_query(client, query)
    blank_row_count = result[0]["blank_row_count"] if result else 0

    status = blank_row_count == 0
    details = {
        'blank_row_count': blank_row_count,
        'message': (
            f"No blank rows in {table_name}"
            if status
            else f"Found {blank_row_count} blank rows in {table_name}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_src_unexpected_nulls(client, schema_name, table_name, test_cols_for_nulls):
    """
    Check for unexpected NULL values in specific columns of an external table
    """
    if not test_cols_for_nulls:
        return {
            'status': True,
            'test_details': {
                'columns_with_nulls': [],
                'message': f"All columns in {table_name} are allowed to be NULL"
            }
        }

    columns_with_nulls = []
    for col in test_cols_for_nulls:
        LOGGER.debug(f"Checking null values in column: {col}")
        null_query = f"""
            SELECT COUNT(*) AS null_count FROM {schema_name}.{table_name} 
            WHERE {col} IS NULL
        """
        result = read_sql_query(client, null_query)
        null_count = result[0]["null_count"] if result else 0
        if null_count > 0:
            columns_with_nulls.append(col)

    status = len(columns_with_nulls) == 0
    details = {
        'columns_with_nulls': columns_with_nulls,
        'message': (
            f"No unexpected NULL values found in {table_name}"
            if status
            else f"Columns with unexpected NULLs: {', '.join(columns_with_nulls)}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_missing_column(client, schema_name, table_name, column_names):
    """
    Check for missing columns in a table

    Args:
        client (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        column_names (list): List of expected columns

    Returns:
        dict: Dictionary containing status and details about missing columns
    """
    query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
    """

    result = read_sql_query(client, query)
    table_columns = [row["column_name"] for row in result] if result else []

    missing_columns = [col for col in column_names if col not in table_columns]

    status = len(missing_columns) == 0
    details = {
        'missing_columns': missing_columns,
        'message': (
            'No missing columns'
            if status
            else f"Missing columns detected: {', '.join(missing_columns)}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_blank_rows(client, schema_name, table_name):
    """
    Identify rows where all values are NULL in a table

    Args:
        client (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table

    Returns:
        dict: Dictionary containing status and details about blank rows
    """
    column_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
    """

    result = read_sql_query(client, column_query)
    table_columns = [row["column_name"] for row in result] if result else []

    if not table_columns:
        return {
            'status': False,
            'test_details': {
                'blank_row_count': None,
                'message': f"Table {table_name} not found or has no columns"
            }
        }

    null_condition = " AND ".join([f"{col} IS NULL" for col in table_columns])

    query = f"""
        SELECT COUNT(*) AS blank_row_count
        FROM {schema_name}.{table_name} 
        WHERE {null_condition}
    """

    result = read_sql_query(client, query)
    blank_row_count = result[0]["blank_row_count"] if result and "blank_row_count" in result[0] else 0

    status = blank_row_count == 0
    details = {
        'blank_row_count': blank_row_count,
        'message': (
            f"No blank rows in {table_name}"
            if status
            else f"Found {blank_row_count} blank rows in {table_name}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def check_unexpected_nulls(client, schema_name, table_name, test_cols_for_nulls):
    """
    Check for NULL values in columns that should not have NULLs

    Args:
        client (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        test_cols_for_nulls (list): List of columns that should NOT have NULL values

    Returns:
        dict: Dictionary containing status and details about unexpected NULL values
    """

    if not test_cols_for_nulls:
        return {
            'status': True,
            'test_details': {
                'columns_with_nulls': [],
                'message': f"All columns in {table_name} are allowed to be NULL"
            }
        }

    columns_with_nulls = []

    for col in test_cols_for_nulls:
        LOGGER.debug(f"Checking null values in column: {col}")

        null_query = f"""
            SELECT COUNT(*) AS null_count FROM {schema_name}.{table_name} 
            WHERE {col} IS NULL
        """
        result = read_sql_query(client, null_query)
        null_count = result[0]["null_count"] if result and "null_count" in result[0] else 0

        if null_count > 0:
            columns_with_nulls.append(col)

    status = len(columns_with_nulls) == 0
    details = {
        'columns_with_nulls': columns_with_nulls,
        'message': (
            f"No unexpected NULL values found in {table_name}"
            if status
            else f"Columns with unexpected NULLs: {', '.join(columns_with_nulls[:5])}"
        )
    }

    return {
        'status': status,
        'test_details': details
    }


def normalize_external_dtype(dtype):
    dtype = dtype.lower().replace(" ", "")
    return dtype


def validate_external_table_schema(client, schema_name, table_name, converted_cols_dict):
    LOGGER.info(f"Starting schema validation for external table {schema_name}.{table_name}")

    query = f"""
        SELECT columnname AS column_name, external_type AS data_type
        FROM svv_external_columns
        WHERE schemaname = '{schema_name}'
        AND tablename = '{table_name}'
    """
    result = read_sql_query(client, query)

    actual_schema = {row["column_name"]: row["data_type"] for row in result} if result else {}
    discrepancies = []

    for col, expected_dtype in converted_cols_dict.items():
        LOGGER.debug(f"Checking data types for column '{col}' in external table...")
        actual_dtype = actual_schema.get(col)

        normalized_expected = normalize_external_dtype(expected_dtype)
        normalized_actual = normalize_external_dtype(actual_dtype) if actual_dtype else None

        if normalized_actual is None:
            discrepancies.append(f"Missing column: '{col}'")
            LOGGER.warning(f"Column '{col}' is missing in external table")
        elif normalized_actual != normalized_expected:
            discrepancies.append(
                f"Type mismatch for '{col}': expected '{expected_dtype}', found '{actual_dtype}'"
            )
            LOGGER.warning(
                f"Datatype mismatch for '{col}': expected '{expected_dtype}', got '{actual_dtype}'"
            )
        else:
            LOGGER.debug(f"Column '{col}' matched successfully")

    status = len(discrepancies) == 0
    details = {
        'discrepancies': discrepancies,
        'message': (
            f"External schema validation successful for {schema_name}.{table_name}"
            if status
            else f"Discrepancies found: {', '.join(discrepancies)}"
        )
    }

    if discrepancies:
        LOGGER.error(details['message'])
    else:
        LOGGER.info(details['message'])

    return {
        'status': status,
        'test_details': details
    }


def normalize_redshift_internal_dtype(dtype):
    dtype = dtype.lower()
    dtype = dtype.replace("character varying", "varchar")
    dtype = dtype.replace("character", "char")
    dtype = dtype.replace("double precision", "double")
    dtype = dtype.replace("timestamp without time zone", "timestamp")
    return dtype


def validate_internal_table_schema(client, schema_name, table_name, converted_cols_dict):
    """
    Validate the schema of an internal table against the expected schema

    Args:
        client (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        converted_cols_dict (dict): Dictionary of expected column names and their data types

    Returns:
        dict: Dictionary containing status and details about schema validation
    """
    LOGGER.info(f"Starting schema validation for internal table {schema_name}.{table_name}")

    query = f"""
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}'
    """
    result = read_sql_query(client, query)

    actual_schema = {}
    if result:
        for row in result:
            col_name = row["column_name"]
            dtype = row["data_type"].lower()

            if dtype in ('character varying', 'varchar', 'character', 'char'):
                length = row["character_maximum_length"]
                actual_dtype = f"{dtype}({length})" if length else dtype
            elif dtype in ('numeric', 'decimal'):
                precision = row["numeric_precision"]
                scale = row["numeric_scale"]
                actual_dtype = f"{dtype}({precision}, {scale})"
            else:
                actual_dtype = dtype

            actual_dtype = normalize_redshift_internal_dtype(actual_dtype)
            actual_schema[col_name] = actual_dtype

    discrepancies = []

    for col, expected_dtype in converted_cols_dict.items():
        LOGGER.debug(f"Checking data types for column '{col}' in internal table...")
        actual_dtype = actual_schema.get(col)

        normalized_expected_dtype = normalize_redshift_internal_dtype(expected_dtype)

        if actual_dtype is None:
            discrepancies.append(f"Missing column: '{col}'")
            LOGGER.warning(f"Column '{col}' is missing in internal table")
        elif actual_dtype != normalized_expected_dtype:
            discrepancies.append(
                f"Type mismatch for '{col}': expected '{normalized_expected_dtype}', found '{actual_dtype}'"
            )
            LOGGER.warning(
                f"Datatype mismatch for '{col}': expected '{normalized_expected_dtype}', got '{actual_dtype}'"
            )
        else:
            LOGGER.debug(f"Column '{col}' matched successfully")

    status = len(discrepancies) == 0
    details = {
        'discrepancies': discrepancies,
        'message': (
            f"Internal schema validation successful for {schema_name}.{table_name}"
            if status
            else f"Discrepancies found: {', '.join(discrepancies)}"
        )
    }

    if discrepancies:
        LOGGER.error(details['message'])
    else:
        LOGGER.info(details['message'])

    return {
        'status': status,
        'test_details': details
    }

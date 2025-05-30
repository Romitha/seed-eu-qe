# utils/framework/data_quality_utils/history_validation_util.py
from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_history_table_existence(client, schema_name, table_name, history_table_name):
    """
    Check if a history table exists for the given main table

    Args:
        client: Database client instance
        schema_name (str): Schema name
        table_name (str): Main table name
        history_table_name (str): History table name

    Returns:
        dict: Dictionary with status and history table name if it exists
    """

    # Log the table names for debugging
    LOGGER.info("🔍 CHECK 1: Verifying history table existence...")

    query = f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
        AND table_name = '{history_table_name}'
    """
    result = read_sql_query(client, query)

    if result:
        return {
            'status': True,
            'history_table_name': history_table_name,
            'message': f"History table {schema_name}.{history_table_name} exists"
        }
    else:
        return {
            'status': False,
            'history_table_name': history_table_name,
            'message': f"History table {schema_name}.{history_table_name} does not exist"
        }


def check_row_counts(client, schema_name, table_name, history_table_name):
    """
    Compare row counts between main table and history table

    Args:
        client: Database client instance
        schema_name (str): Schema name
        table_name (str): Main table name
        history_table_name (str): History table name

    Returns:
        dict: Dictionary containing row count comparison results
    """
    LOGGER.info("🔍 CHECK 2: Comparing row counts between main and history tables...")
    query = f"""
        SELECT 
            (SELECT COUNT(*) FROM {schema_name}.{table_name}) AS main_count,
            (SELECT COUNT(*) FROM {schema_name}.{history_table_name}) AS history_count
    """

    result = read_sql_query(client, query)
    if not result:
        return {
            'status': False,
            'message': f"Failed to retrieve row counts for {table_name} and {history_table_name}"
        }

    main_count = result[0].get('main_count', 0)
    history_count = result[0].get('history_count', 0)
    # For truncate-load, history should have at least as many rows as main
    status = history_count >= main_count

    return {
        'status': status,
        'main_count': main_count,
        'history_count': history_count,
        'message': (
            f"History table has sufficient records ({history_count} >= {main_count})"
            if status
            else f"History table has insufficient records ({history_count} < {main_count})"
        )
    }


def check_latest_history_matches(client, schema_name, table_name, history_table_name, unique_columns):
    """
    Check if latest history records match current main table records
    First checks record counts, then performs detailed matching using unique columns

    Args:
        client: Database client instance
        schema_name (str): Schema name
        table_name (str): Main table name
        history_table_name (str): History table name
        unique_columns (list): List of unique columns to use for matching

    Returns:
        dict: Dictionary containing match results
    """
    LOGGER.info("🔍 CHECK 3: Verifying latest history records match main table...")
    # Step 1: Check record counts first
    count_check_query = f"""
    WITH main_table_count AS (
        SELECT COUNT(*) AS total_records 
        FROM {schema_name}.{table_name}
    ),
    history_table_latest_date AS (
        SELECT MAX(insrt_dttm) AS latest_insert
        FROM {schema_name}.{history_table_name}
    ),
    history_table_latest_count AS (
        SELECT COUNT(*) AS total_records
        FROM {schema_name}.{history_table_name}
        WHERE insrt_dttm = (SELECT latest_insert FROM history_table_latest_date)
    )
    SELECT 
        m.total_records AS main_table_records,
        h.total_records AS latest_history_records,
        CASE 
            WHEN m.total_records = h.total_records THEN 'Records match'
            ELSE 'Records do not match'
        END AS status,
        (SELECT latest_insert FROM history_table_latest_date) AS latest_history_date,
        CASE 
            WHEN m.total_records = h.total_records THEN 'All records from main table exist in latest history snapshot'
            ELSE CONCAT(ABS(m.total_records - h.total_records), ' records missing or extra in history')
        END AS details
    FROM main_table_count m, history_table_latest_count h
    """

    count_result = read_sql_query(client, count_check_query)

    if not count_result:
        return {
            'status': False,
            'message': f"Failed to get record counts for {table_name} and {history_table_name}"
        }

    main_count = count_result[0].get('main_table_records', 0)
    history_count = count_result[0].get('latest_history_records', 0)
    latest_date = count_result[0].get('latest_history_date')
    details = count_result[0].get('details', '')

    # If record counts don't match, return immediately
    if main_count != history_count:
        return {
            'status': False,
            'main_count': main_count,
            'history_count': history_count,
            'latest_history_date': latest_date,
            'message': f"Record count mismatch: {details}"
        }

    # Step 2: If counts match, perform detailed matching using unique columns
    if not unique_columns:
        return {
            'status': False,
            'message': "No unique columns provided for detailed matching"
        }

    # Function to properly quote column names
    def quote_col(col):
        if col[0].isdigit() or '-' in col or ' ' in col or any(c for c in col if not (c.isalnum() or c == '_')):
            return f'"{col}"'
        return col

    # Build join conditions for unique columns with NULL handling
    join_conditions = []
    for col in unique_columns:
        quoted_col = quote_col(col)
        # Handle potential NULL values in joins
        join_conditions.append(
            f"(a.{quoted_col} = h.{quoted_col} OR (a.{quoted_col} IS NULL AND h.{quoted_col} IS NULL))")

    join_cond = " AND ".join(join_conditions)

    # Use the minimum of main_count and sample_limit for the LIMIT
    # limit_value = min(main_count, sample_limit)
    limit_value = int(main_count)

    detailed_match_query = f"""
    WITH latest_hist_record AS (
        SELECT *
        FROM {schema_name}.{history_table_name}
        WHERE insrt_dttm = (
            SELECT MAX(insrt_dttm)
            FROM {schema_name}.{history_table_name}
        )
        LIMIT {limit_value}
    )
    SELECT COUNT(*) AS matched_count
    FROM {schema_name}.{table_name} a
    JOIN latest_hist_record h
    ON {join_cond}
    """

    match_result = read_sql_query(client, detailed_match_query)

    if not match_result:
        return {
            'status': False,
            'main_count': main_count,
            'history_count': history_count,
            'latest_history_date': latest_date,
            'message': f"Failed to perform detailed matching between {table_name} and {history_table_name}"
        }

    matched_count = match_result[0].get('matched_count', 0)
    expected_matches = limit_value

    # Check if all expected matches were found
    all_matched = matched_count == expected_matches

    return {
        'status': all_matched,
        'main_count': main_count,
        'history_count': history_count,
        'matched_count': matched_count,
        'expected_matches': expected_matches,
        'sample_limit': limit_value,
        'latest_history_date': latest_date,
        'unique_columns_used': unique_columns,
        'message': (
            f"Perfect match: All {matched_count} records matched using unique columns"
            if all_matched
            else f"Partial match: Only {matched_count} out of {expected_matches} records matched using unique columns"
        )
    }


def check_history_timestamps(client, schema_name, history_table_name):
    """
    Check the timestamp progression in history table

    Args:
        client: Database client instance
        schema_name (str): Schema name
        history_table_name (str): History table name

    Returns:
        dict: Dictionary containing timestamp analysis results
    """
    LOGGER.info("🔍 CHECK 4: Verifying history timestamp progression...")
    query = f"""
        SELECT 
            COUNT(DISTINCT insrt_dttm) AS distinct_timestamps,
            MIN(insrt_dttm) AS first_timestamp,
            MAX(insrt_dttm) AS last_timestamp
        FROM {schema_name}.{history_table_name}
    """

    result = read_sql_query(client, query)

    if not result:
        return {
            'status': False,
            'message': f"Failed to analyze timestamps in {history_table_name}"
        }

    distinct_timestamps = result[0].get('distinct_timestamps', 0)
    first_timestamp = result[0].get('first_timestamp')
    last_timestamp = result[0].get('last_timestamp')

    # For a truncate-load table with active history, we expect at least one timestamp
    # Changed from > 1 to >= 1 to accommodate first-time loads
    return {
        'status': distinct_timestamps >= 1,
        'distinct_timestamps': distinct_timestamps,
        'first_timestamp': first_timestamp,
        'last_timestamp': last_timestamp,
        'message': (
            f"History table shows proper versioning with {distinct_timestamps} distinct timestamps"
            if distinct_timestamps >= 1
            else f"History table has insufficient versioning ({distinct_timestamps} timestamps)"
        )
    }
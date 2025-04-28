import re

from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def validate_rules(client, validation_rules, schema_name, table_name, expected_columns):
    """
    Validates a set of rules against a database table

    Args:
        client: Database client instance
        validation_rules (dict): Dictionary containing validation rules
        schema_name (str): Schema name
        table_name (str): Table name
        expected_columns (list): List of expected columns in the table

    Returns:
        dict: Dictionary containing status and validation details
    """

    rules = {
        "regex_match": "CASE WHEN {column} IS NULL OR {column} = '' THEN 0 "
                       "WHEN REGEXP_COUNT({column}, '{value}') > 0 THEN 0 ELSE 1 END",

        "value_equal": "CASE WHEN {column} IS NULL OR {column} = '' THEN 0 "
                       "WHEN {column} = '{value}' THEN 0 ELSE 1 END",

        "value_greater_than": "CASE WHEN {column} IS NULL THEN 0 "
                              "WHEN {column} > {value} THEN 0 ELSE 1 END"
    }

    query_array = []
    test_details = {}

    # Ensure expected_columns contain only valid column names
    expected_columns = [re.match(r'^\S+', col).group() for col in expected_columns]

    # Validate input is a dictionary
    if not validation_rules or not isinstance(validation_rules, dict):
        LOGGER.warning(f"Rule based validation skipped: No validation rules set for {table_name}")
        return {
            'status': "Skipped",
            'test_details': {
                'message': "Invalid validation rules provided or empty dictionary"
            }
        }

    for column, rule_dict in validation_rules.items():
        if column in expected_columns:
            for rule, value in rule_dict.items():
                if rule in rules:
                    sub_query_str = rules[rule].format(column=column, value=value)
                    query_str = f"SUM({sub_query_str}) AS \"{column} with {rule}: {value} rule\""
                    query_array.append(query_str)
                else:
                    test_details[column] = {
                        'status': False,
                        'message': f"Rule '{rule}' is not defined"
                    }
        else:
            test_details[column] = {
                'status': False,
                'message': f"Column '{column}' is not a valid column"
            }

    # If no valid query parts exist, return failure
    if not query_array:
        if test_details:
            return {
                'status': False,
                'test_details': test_details
            }
        else:
            return {
                'status': False,
                'test_details': {
                    'message': "No valid validation rules to apply"
                }
            }

    # Build final SQL query
    query_array_str = ', '.join(query_array)

    if query_array_str:
        final_query = f"SELECT {query_array_str}, COUNT(*) AS row_count FROM {schema_name}.{table_name}"
    else:
        final_query = f"SELECT COUNT(*) AS row_count FROM {schema_name}.{table_name}"

    try:
        results = read_sql_query(client, final_query)

        if not results or not results[0]:  # Ensure results exist and are not empty
            return {
                'status': False,
                'test_details': {
                    'message': "Query execution returned no results"
                }
            }

        row_count = results[0].get("row_count", 0)

        if row_count and row_count > 0:
            LOGGER.info(f"Total records checked in {table_name}: {row_count}")
            # Parse results into structured format
            for key, value in results[0].items():
                if key == 'row_count':
                    continue
                clean_key = key.strip()  # Remove unnecessary spaces
                status = value == 0
                test_details[clean_key] = {
                    'status': status,  # If SUM() returns 0, it means no validation failures
                    'failures': value,
                    'records_count': row_count,
                    'message': f"{clean_key} had {value} validation failures"
                }
                if status:
                    LOGGER.info(f"Validation rule passed for {clean_key}")
                else:
                    LOGGER.warning(
                        f"Validation rule failed for {clean_key}. "
                        f"Detected {value} violations out of {row_count} records"
                    )
        else:
            LOGGER.warning(f"Validation skipped: No records found in {table_name}")
            return {
                'status': True,
                'test_details': {
                    'message': f"Test has been skipped due to no records"
                }
            }

        return {
            'status': all(detail['status'] for detail in test_details.values()),
            'test_details': test_details
        }

    except Exception as e:
        return {
            'status': False,
            'test_details': {
                'message': f"Error executing validation query: {str(e)}"
            }
        }

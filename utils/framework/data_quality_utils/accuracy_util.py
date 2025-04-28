from sqlalchemy.exc import SQLAlchemyError

from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_numeric_precision_for_column(client, schema_name, table_name, column, settings):
    """
    Check numeric precision and scale for a column in a database table

    Args:
        client: Database client connection
        schema_name (str): Schema name
        table_name (str): Table name
        column (str): Column name
        settings (dict): Expected precision settings containing 'num_precision' and 'numeric_scale'

    Returns:
        dict: Dictionary containing status and details about precision/scale validation
    """

    query = f"""
        SELECT numeric_precision, numeric_scale 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}' 
        AND column_name = '{column}'
    """

    try:
        result = read_sql_query(client, query)

        if not result:
            return {
                'status': False,
                'test_details': {
                    'column': column,
                    'expected_precision': settings.get('num_precision'),
                    'expected_scale': settings.get('numeric_scale'),
                    'message': f"Column '{column}' is not found in table '{schema_name}.{table_name}'"
                }
            }

        row = result[0]
        db_precision = row.get('numeric_precision')
        db_scale = row.get('numeric_scale')

        expected_precision = settings.get('num_precision')
        expected_scale = settings.get('numeric_scale')

        mismatches = []

        if expected_precision is not None and db_precision != expected_precision:
            mismatches.append(
                f"Precision mismatch: expected {expected_precision}, got {db_precision}"
            )

        if expected_scale is not None and db_scale != expected_scale:
            mismatches.append(
                f"Scale mismatch: expected {expected_scale}, got {db_scale}"
            )

        status = len(mismatches) == 0
        details = {
            'column': column,
            'expected_precision': expected_precision,
            'expected_scale': expected_scale,
            'actual_precision': db_precision,
            'actual_scale': db_scale,
            'message': (
                f"Precision and scale match expected values ({expected_precision}, {expected_scale})"
                if status
                else f"Column '{column}' in table '{schema_name}.{table_name}' has mismatches: " + "; ".join(mismatches)
            )
        }

        return {
            'status': status,
            'test_details': details
        }

    except SQLAlchemyError as e:
        return {
            'status': False,
            'test_details': {
                'column': column,
                'expected_precision': settings.get('num_precision'),
                'expected_scale': settings.get('numeric_scale'),
                'message': f"Database error while checking column '{column}': {str(e)}"
            }
        }

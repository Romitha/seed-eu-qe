from sqlalchemy.exc import SQLAlchemyError

from utils.common.sqlalchemy_util import read_sql_query
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def check_timeliness_in_latest_batch(
        engine, schema_name, table_name, sys_dt_col, expected_within_hours
):
    """
    Check if the latest batch of system insert/update records falls within the expected
    time frame

    Args:
        engine (Any): Database client instance
        schema_name (str): Name of the schema
        table_name (str): Name of the table
        sys_dt_col (str): System timestamp column used for timeliness check
        expected_within_hours (int): Maximum allowed age of the latest batch (in hours)

    Returns:
        dict: Dictionary containing status and details about timeliness
    """

    try:
        query = f"""
            SELECT MAX({sys_dt_col}) AS latest_insert
            FROM {schema_name}.{table_name}
        """
        result = read_sql_query(engine, query)
        latest_sys_time = result[0]["latest_insert"] if result else None

        if latest_sys_time:
            time_diff_query = f"""
                SELECT DATEDIFF(hour, TIMESTAMP '{latest_sys_time}', GETDATE()) 
                AS hours_difference
            """
            time_diff_result = read_sql_query(engine, time_diff_query)
            hours_difference = time_diff_result[0]["hours_difference"] if time_diff_result else None

            status = hours_difference is not None and hours_difference <= expected_within_hours

            details = {
                'latest_insert_time': latest_sys_time,
                'hours_difference': hours_difference,
                'message': (
                    f"Insert timeliness for the latest batch in {table_name} is within "
                    f"the expected {expected_within_hours} hours"
                    if status
                    else f"Latest insert in {table_name} is {hours_difference} hours old, "
                         f"exceeding the expected {expected_within_hours} hours"
                )
            }

            if not status:
                LOGGER.warning(details['message'])

            return {
                'status': status,
                'test_details': details
            }

        else:
            LOGGER.warning(f"No data found in {schema_name}.{table_name}")
            return {
                'status': False,
                'test_details': {
                    'latest_insert_time': None,
                    'hours_difference': None,
                    'message': f"No data found in {table_name}"
                }
            }

    except SQLAlchemyError as e:
        error_message = f"Error during insert timeliness check for latest batch: {str(e)}"
        LOGGER.error(error_message)
        return {
            'status': False,
            'test_details': {
                'latest_insert_time': None,
                'hours_difference': None,
                'message': error_message
            }
        }

import pytest

from data_processing.data_processor import DataProcessor
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def test_generic_data_flow(config_fixture, all_tables_test_results):
    """
    Automate data loading flow for testing purposes

    Args:
        config_fixture: Configuration fixture for settings
    """
    LOGGER.debug(config_fixture.settings)

    # Initialize DataProcessor with config and process
    processor = DataProcessor(config_fixture.settings)
    table_test_results = processor.process()

    if table_test_results:
        table_name = next(iter(table_test_results))
        if table_test_results[table_name]:
            LOGGER.info(f"Test results for table {table_name}: %s", table_test_results)
        else:
            pytest.skip(f"All tests for table {table_name} have been skipped")
        all_tables_test_results.update(table_test_results)
    else:
        pytest.skip("No test results available to process.")

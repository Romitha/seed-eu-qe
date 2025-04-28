from connection.connection_manager import ConnectionManager
from utils.common.s3_util import count_s3_files, delete_s3_files
from utils.common.spectrum_util import (
    check_table_exists_in_external_db,
    delete_spectrum_table_in_catalog_if_exists)
from utils.common.synthetic_data_util import delete_synthetic_data
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class DataCleanupHelper:
    def __init__(self, config):
        """
        Initialize the WarehouseStrategyHelper with the provided configuration.

        Args:
            config (dict): Configuration dictionary.
        """
        self.config = config
        self.connection_mgr = ConnectionManager()

        self.source_bucket = config.get('source_data_bucket_name')
        self.run_mode = config.get("run_mode")
        self.table_name = config.get("table_name")
        self.table_settings = config.get(self.table_name, {})
        self.connection_system = next(iter(self.table_settings), None)
        self.column_info = self.table_settings.get("columns_info", {})
        self.expected_columns = self.column_info.get("expected_columns", {})
        self.conf_synthetic_data = self.table_settings.get("synthetic_data", {})

    def check_and_run_test_data_clean_up(self, wh_client, src_client, ext_db_client, layer_name, layer_info,
                                         src_layer_settings, has_synthetic_data):

        layer_settings = layer_info['layer_settings']

        # clean up glue_db tables created for testing (RR)
        spectrum_table_name = layer_settings.get('table_identifier')
        source_table_settings = src_layer_settings
        external_db_name = source_table_settings.get('external_db')
        ext_table_exists = check_table_exists_in_external_db(ext_db_client, external_db_name, spectrum_table_name)
        if ext_table_exists:
            delete_spectrum_table_in_catalog_if_exists(ext_db_client, external_db_name, spectrum_table_name)

        # revert s3 files back to versions
        s3_client = src_client
        src_bucket = self.source_bucket
        src_uri = self.table_settings[self.connection_system]['source']['uri']

        file_count_in_s3 = count_s3_files(s3_client, src_bucket, src_uri)

        if file_count_in_s3 > 0:
            LOGGER.info(f" Totally {file_count_in_s3} files found for deletion in source location {src_uri}")
            delete_s3_files(s3_client, src_bucket, src_uri)

        # if confirmed for synthetic data gen or not allowed to discard, ignore synthetic data deletion
        allowed_to_delete = has_synthetic_data or self.conf_synthetic_data.get('discard_data')
        if not allowed_to_delete:
            return

        schema_name = layer_settings.get('schema_name')
        table_name = layer_settings.get("table_name")

        if layer_name != 'source':
            if layer_settings.get("load_strategy") != 'scd':
                delete_synthetic_data(wh_client, schema_name, table_name)
            elif layer_name == 'target_lndp':
                delete_synthetic_data(wh_client, schema_name, table_name)
            elif layer_name == 'target_edwp':
                delete_synthetic_data(wh_client, schema_name, table_name)
                table_name = layer_settings['lndp_settings']['table_name']
                schema_name = layer_settings['lndp_settings']['schema_name']
                delete_synthetic_data(wh_client, schema_name, table_name)

        elif layer_name == 'source':
            LOGGER.info(f"Source layer is not supported for synthetic data deletion/creation")

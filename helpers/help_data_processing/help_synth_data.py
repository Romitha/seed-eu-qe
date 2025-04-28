from connection.connection_manager import ConnectionManager
from utils.common.synthetic_data_util import (
    generate_synthetic_data, generate_table_schema_from_columns,
    insert_synthetic_data)
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class SynthDataHelper:
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
        self.confirm_synth_data_gen = None

    def initiate_synthetic_data_gen(self, test_layer, db_client, layer_settings):

        # currently this is just logging debug, but fc should be to validate conf_synthetic_data (RR)
        LOGGER.debug(f"Synthetic data generation config set as: {self.conf_synthetic_data}")

        scd_settings = layer_settings.get("scd_settings")

        if scd_settings and test_layer == 'edwp':

            # special case hence get the lndp table and schema to generate
            lndp_table_name = layer_settings['lndp_settings']['table_name']
            lndp_schema_name = layer_settings['lndp_settings']['schema_name']

            expected_columns = self.column_info.get('expected_columns')
            has_mapping = layer_settings['has_mapping']

            columns = layer_settings['mapped_expected_cols'] if has_mapping else expected_columns

            # generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {lndp_table_name} table {lndp_schema_name}")

            # pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # finally insert into the relevant table
            insert_synthetic_data(db_client, lndp_schema_name, lndp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for reference layer since SCD table {lndp_table_name}")

        elif not scd_settings and test_layer == 'edwp':
            edwp_table_name = layer_settings.get("table_name")
            edwp_schema_name = layer_settings.get("schema_name")

            expected_columns = self.column_info.get('expected_columns')
            has_mapping = layer_settings['has_mapping']

            columns = layer_settings['mapped_expected_cols'] if has_mapping else expected_columns

            # generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {edwp_schema_name} table {edwp_table_name}")

            # pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # finally insert into the relevant table
            insert_synthetic_data(db_client, edwp_schema_name, edwp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for target layer table {edwp_table_name}")

        elif not scd_settings and test_layer == 'lndp':
            lndp_table_name = layer_settings.get("table_name")
            lndp_schema_name = layer_settings.get("schema_name")

            expected_columns = self.column_info.get('expected_columns')
            has_mapping = layer_settings['has_mapping']

            columns = layer_settings['mapped_expected_cols'] if has_mapping else expected_columns

            # generate the schema and rows
            table_schema = generate_table_schema_from_columns(columns)
            rows = self.conf_synthetic_data.get('row_count')

            LOGGER.info(f"Generating synthetic data for target schema {lndp_table_name} table {lndp_schema_name}")

            # pass the generated schema and rows to the data generate method
            synthetic_data = generate_synthetic_data(table_schema, rows)

            # finally insert into the relevant table
            insert_synthetic_data(db_client, lndp_schema_name, lndp_table_name, synthetic_data)
            LOGGER.info(f"Synthetic data generation completed for target table {lndp_table_name}")

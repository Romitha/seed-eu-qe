from utils.common.s3_util import (check_s3_files_exist,
                                  list_recoverable_s3_file_versions)
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class WarehouseStrategyHelper:
    def __init__(self, config):
        """
        Initialize the WarehouseStrategyHelper with the provided configuration.

        Args:
            config (dict): Configuration dictionary.
        """
        self.config = config
        self.source_bucket = config.get('source_data_bucket_name')
        self.run_mode = config.get("run_mode")
        self.table_name = config.get("table_name")
        self.table_settings = config.get(self.table_name, {})
        self.connection_system = next(iter(self.table_settings), None)
        self.column_info = self.table_settings.get("columns_info", {})
        self.expected_columns = self.column_info.get("expected_columns", {})
        self.conf_synthetic_data = self.table_settings.get("synthetic_data", {})
        self.test_scope = self.table_settings.get("test_scope", {})
        self.test_info = self.table_settings.get("test_info", {})
        self.load_strategy = self.test_info.get('load_strategy')
        self.scd_info = self.table_settings.get("scd_info", {})
        self.enable_scd_validations = self.scd_info.get("enable_scd_validations", {})
        self.user_enabled_synthetic_data = self.test_info.get('use_synthetic_data')
        self.confirm_synth_data_gen = None

    def check_table_settings_and_proceed(self):
        if not self.table_settings:
            LOGGER.error(f"No settings found for table: {self.table_name}")
            return False
        if self.load_strategy not in ['truncate_load', 'scd']:
            LOGGER.error(f"Unsupported loading strategy provided for table: {self.table_name}")
            return False
        else:
            return True

    def get_test_table_name(self):
        return self.table_name

    def get_load_strategy(self):
        return self.load_strategy

    def get_connection_system_name(self):
        return self.connection_system

    def get_source_layer_settings(self):
        return self.table_settings.get(self.connection_system).get('source')

    def get_enabled_layers_and_settings_to_test(self):
        """
        Determines which layers are enabled for testing and captures settings (RR)

        Returns:
            dict: layers in scope and their settings
        """
        enabled_layers = {}

        scd_validations = self.scd_info.get('validation')

        # Check if SCD table in that case we can add that scope as well before proceeding
        if self.load_strategy == 'scd' and self.enable_scd_validations:

            LOGGER.info(f"SCD table detected - adding scd validations: {scd_validations} to final target layer")

            if self.test_scope['target_edwp']['data_validation'] is not None:
                self.test_scope['target_edwp']['data_validation'].extend(scd_validations)
            else:
                self.test_scope['target_edwp']['data_validation'] = scd_validations

        # Check if all layers have scopes with only None values, if so return
        if all(all(value is None for value in scope.values()) for scope in self.test_scope.values()):
            return enabled_layers

        for layer, scope in self.test_scope.items():
            # Skip layers where all keys in the scope are None
            if all(value is None for value in scope.values()):
                continue

            if layer == 'source':
                layer_active_settings = self.table_settings.get(self.connection_system, {}).get(layer, {})
            elif layer.startswith('target'):
                target_key = layer.split('_')[-1]
                layer_active_settings = self.table_settings.get(self.connection_system, {}).get('target', {}).get(
                    target_key, {})
            else:
                layer_active_settings = {}

            scope_and_layer_settings = {
                "scope": {key: value for key, value in scope.items() if value is not None},
                "layer_settings": layer_active_settings
            }

            enabled_layers[layer] = scope_and_layer_settings

            LOGGER.debug(
                f"Layer {layer} is enabled for testing with combined settings: {scope_and_layer_settings}"
            )

        return enabled_layers

    def check_requires_synthetic_data_generation(self, src_client):

        src_settings = self.get_source_layer_settings()
        uri = src_settings.get('uri')

        s3_has_data = check_s3_files_exist(src_client, self.source_bucket, uri)
        s3_has_recoverable_data = list_recoverable_s3_file_versions(src_client, self.source_bucket, uri)

        if self.user_enabled_synthetic_data:
            self.confirm_synth_data_gen = True

        elif not s3_has_data and not s3_has_recoverable_data:
            self.confirm_synth_data_gen = True

        return self.confirm_synth_data_gen

    def is_unsupported_source_layer(self, src_client):
        data_gen_status = self.check_requires_synthetic_data_generation(src_client)

        if data_gen_status:
            return True

        if self.scd_info.get("has_opco"):
            LOGGER.info("Skipping source layer tests because Conform table is not supported for this layer")
            return True

        return None

    def process_verification_results(self, results, layer_name):
        """
        Recursively searches for a table and its layer in results.
        If any sub-dictionaries under layer_name contain "status": False,
        it raises an error and logs the associated test details.
        """
        table_name = self.table_name

        # Step 1: Validate if the table_name exists
        if table_name not in results:
            raise KeyError(f"Table '{table_name}' is not found in results")

        # Step 2: Validate if the layer_name exists under the table
        if layer_name not in results[table_name]:
            raise KeyError(f"Layer '{layer_name}' not found under table '{table_name}'")

        # Step 3: Function to recursively check for 'status': False in nested dictionaries
        def find_failed_status(sub_dict):
            # Recursively looks for 'status': False in nested dictionaries
            if not isinstance(sub_dict, dict):
                return None  # Skip non-dictionary values

            # If 'status' key exists and is False, return test details
            if sub_dict.get("status") is False:
                return sub_dict.get("test_details", "No test details available")

            # Recursively search in all dictionary values
            for value in sub_dict.values():
                error_message = find_failed_status(value)
                if error_message:
                    return error_message  # Return immediately once an error is found

            return None

        # Step 4: Start checking within the specified layer
        error_details = find_failed_status(results[table_name][layer_name])

        if error_details:
            raise ValueError(f"Verification failed for table '{table_name}', layer '{layer_name}': {error_details}")

        LOGGER.info(f"Table '{table_name}', Layer '{layer_name}' passed verification")

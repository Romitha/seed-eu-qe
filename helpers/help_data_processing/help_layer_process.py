from utils.framework.custom_data_processing_util import (
    get_col_mapping_for_layer, initiate_spectrum_creation)
from utils.framework.custom_data_verification_util import generate_lndp_and_edwp_col_values
from utils.framework.custom_logger_util import get_logger
from utils.framework.data_validation_utils.scd_util import get_scd_default_cols

LOGGER = get_logger()


class LayerProcessHelper:
    def __init__(self, config, layer_name, layer_info):
        self.config = config
        self.layer_name = layer_name
        self.layer_info = layer_info
        self.source_bucket = config.get('source_data_bucket_name')
        self.run_mode = config.get("run_mode")
        self.table_name = config.get("table_name")
        self.table_settings = config.get(self.table_name, {})
        self.connection_system = next(iter(self.table_settings), None)
        self.column_info = self.table_settings.get("columns_info", {})
        self.expected_columns = self.column_info.get("expected_columns", {})
        self.lndp_columns, self.ewdp_columns = generate_lndp_and_edwp_col_values(self.expected_columns)
        self.conf_synthetic_data = self.table_settings.get("synthetic_data", {})
        self.test_scope = self.table_settings.get("test_scope", {})
        self.test_info = self.table_settings.get("test_info", {})
        self.scd_info = self.table_settings.get("scd_info", {})
        self.enable_scd_validations = self.scd_info.get("enable_scd_validations", {})
        self.user_enabled_synthetic_data = self.test_info.get('use_synthetic_data')
        self.load_strategy = self.test_info.get('load_strategy')
        self.spectrum_schema = (
            self.table_settings.get(self.connection_system, {}).get('source', {}).get('spectrum_schema', None))

    def setup_additional_layer_settings(self, test_layer, layer_info, is_src_supported, data_gen_status):

        # start by adding the mapped expected cols settings only if its present else set it false
        mapped_columns = self.column_info.get('mapped_cols', {})
        mapped_expected_cols = mapped_columns.get(test_layer) if mapped_columns else None

        if mapped_expected_cols:
            layer_info['layer_settings'].update({
                'has_mapping': True,
                'mapped_expected_cols': mapped_expected_cols
            })
        else:
            layer_info['layer_settings']['has_mapping'] = False

        layer_info['layer_settings']['no_src_support'] = is_src_supported

        if test_layer == 'source':
            layer_info['layer_settings']['spectrum_schema'] = self.spectrum_schema
            layer_info['layer_settings']['table_identifier'] = self.table_name
            layer_info['layer_settings']['load_strategy'] = self.load_strategy
            layer_info['layer_settings']['columns_info'] = self.column_info
            layer_info['layer_settings']['run_mode'] = self.run_mode

        if test_layer in ['lndp', 'edwp']:
            layer_info['layer_settings']['spectrum_schema'] = self.spectrum_schema
            layer_info['layer_settings']['table_identifier'] = self.table_name
            layer_info['layer_settings']['load_strategy'] = self.load_strategy
            layer_info['layer_settings']['columns_info'] = self.column_info
            layer_info['layer_settings']['run_mode'] = self.run_mode
            layer_info['layer_settings']['confirm_synth_data_gen'] = data_gen_status

        if self.load_strategy == 'scd' and test_layer == 'edwp':
            layer_info['layer_settings']['scd_settings'] = self.scd_info
            layer_info['layer_settings']['scd_default_columns'] = get_scd_default_cols(self.scd_info)
            layer_info['layer_settings']['git_pat'] = self.config.get('seed-eu-git-pat')

    def run_source_layer_process(
            self, wh_client, src_client, ext_db_client,
            layer_name, layer_info, is_src_supported, src_layer_settings, data_gen_status):

        layer_settings = layer_info['layer_settings']

        test_layer = layer_name.rsplit("_", 1)[-1]

        LOGGER.info(f"Assembling test layer for source {test_layer} with settings: {layer_settings}")

        self.setup_additional_layer_settings(test_layer, layer_info, is_src_supported, data_gen_status)

        if not data_gen_status:
            initiate_spectrum_creation(
                wh_client, src_client, ext_db_client, src_layer_settings, self.table_name,
                self.lndp_columns, self.source_bucket)
        else:
            LOGGER.info("Cannot support source layer synthetic data generation yet")

    def run_target_layer_process(
            self, db_client, src_client, ext_db_client,
            layer_name, layer_info, is_src_supported, src_layer_settings, data_gen_status):

        layer_settings = layer_info['layer_settings']

        LOGGER.debug(f"Assembling test layer for target with settings: {layer_settings}")

        reference_layer = layer_settings.get('reference_layer')
        test_layer = layer_name.rsplit("_", 1)[-1]

        self.column_info['mapped_cols'] = get_col_mapping_for_layer(reference_layer, test_layer, self.column_info)

        self.setup_additional_layer_settings(test_layer, layer_info, is_src_supported, data_gen_status)

        if reference_layer == "spectrum" and not is_src_supported:
            initiate_spectrum_creation(
                db_client, src_client, ext_db_client, src_layer_settings, self.table_name,
                self.lndp_columns, self.source_bucket)

        elif reference_layer == "lndp":
            lndp_settings = self.table_settings.get(self.connection_system).get('target').get('lndp')
            layer_info['layer_settings']['lndp_settings'] = lndp_settings

        else:
            LOGGER.info("Unsupported or invalid reference layer provided")

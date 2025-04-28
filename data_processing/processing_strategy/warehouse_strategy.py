import copy
import logging

from helpers.help_connection.help_db_connection import ConnectionHelper
from helpers.help_data_processing.help_data_cleanup import DataCleanupHelper
from helpers.help_data_processing.help_layer_process import LayerProcessHelper
from helpers.help_data_processing.help_synth_data import SynthDataHelper
from helpers.help_data_processing.help_warehouse_strategy import \
    WarehouseStrategyHelper
from helpers.help_data_verification.help_builder_command import \
    BuilderCommandHelper

LOGGER = logging.getLogger(__name__)


class WarehouseStrategy:
    """
    WarehouseStrategy is responsible for executing data processing based on SQL/Warehouse
    strategies - gets the connections required and handles test layers
    """

    def __init__(self, config):
        """
        Initialize the WarehouseStrategy with the provided configuration

        Args:
            config (dict): Configuration containing table settings and processing info
        """
        self.wh_client = None
        self.src_client = None
        self.ext_db_client = None
        self.data_gen_status = None
        self.src_layer_settings = None

        # Deep copies the configuration to ensure no accidental changes take place to original config
        self.config = copy.deepcopy(config)

        # Leverage the helper class to carry out complex logics
        self.wh_strat_helper = WarehouseStrategyHelper(self.config)
        self.con_helper = ConnectionHelper()

        # Collect and store all finalized results, and can be passed in future modules, like assert, report, etc.
        self.results = {}

    def execute(self):
        """
        Execute the data processing logic using the selected strategy
        and retrieves layers enabled for testing
        and processes each of them
        """

        if not self.wh_strat_helper.check_table_settings_and_proceed():
            return

        # get the table name
        table_name = self.wh_strat_helper.get_test_table_name()
        LOGGER.info(f"Starting tests for table: {table_name}")

        # get the connection name for debug purposes
        connection_system = self.wh_strat_helper.get_connection_system_name()
        LOGGER.debug(f"Using connection system: {connection_system}")

        # get source layer settings
        self.src_layer_settings = self.wh_strat_helper.get_source_layer_settings()

        # get the warehouse connected client
        self.wh_client = self.con_helper.get_connected_data_wh_client(connection_system, self.config)

        # get the src connected client
        self.src_client = self.con_helper.get_connected_src_storage_client(self.src_layer_settings, self.config)

        # get the glue connected client
        self.ext_db_client = self.con_helper.get_connected_ext_db_client(self.src_layer_settings, self.config)

        # check and store the status for synthetic data generation
        self.data_gen_status = self.wh_strat_helper.check_requires_synthetic_data_generation(self.src_client)

        # get all enabled layers to test
        layers_to_test = self.wh_strat_helper.get_enabled_layers_and_settings_to_test()

        # if there is 1 or more layer then process each of them accordingly
        if layers_to_test:
            self.process_enabled_test_layers(table_name, layers_to_test)
        else:
            LOGGER.info(f"No layers enabled for testing in this table: {table_name}")

        return self.results

    def process_enabled_test_layers(self, table_name, layers_to_test):
        """
        Processes each enabled layer by logging and handling its test scope and settings

        Args:
            table_name (str) : The unique table name
            layers_to_test (dict): Enabled layers with their combined settings
        """

        self.results.update({table_name: {}})

        is_src_supported = self.wh_strat_helper.is_unsupported_source_layer(self.src_client)

        layer_names = list(layers_to_test.keys())
        total_layers = len(layer_names)

        LOGGER.info(f"Enabled layer count for testing: {total_layers}")
        LOGGER.info(f"Enabled layer names: {', '.join(layer_names)}")

        for layer_name, layer_info in layers_to_test.items():

            LOGGER.info(f"----------------------{layer_name}------------------------")
            LOGGER.info(f"Proceeding with enabled layer: {layer_name}")

            layer_helper = LayerProcessHelper(self.config, layer_name, layer_info)
            synth_data_helper = SynthDataHelper(self.config)
            builder_helper = BuilderCommandHelper()
            data_clr_helper = DataCleanupHelper(self.config)

            if layer_name == 'source':
                if self.wh_strat_helper.is_unsupported_source_layer(self.src_client):
                    LOGGER.info("Skipping source layer tests because data generation "
                                " or Conform table is not supported for this layer")
                    continue
                LOGGER.info("Start processing for source layer")
                layer_helper.run_source_layer_process(
                    self.wh_client, self.src_client, self.ext_db_client,
                    layer_name, layer_info, is_src_supported, self.src_layer_settings, self.data_gen_status)

            elif layer_name.startswith('target'):
                LOGGER.info("Start processing for target layer")
                layer_helper.run_target_layer_process(
                    self.wh_client, self.src_client, self.ext_db_client,
                    layer_name, layer_info, is_src_supported, self.src_layer_settings, self.data_gen_status)
            else:
                raise ValueError(f"Unknown or unsupported layer provided: {layer_name}")

            if self.data_gen_status:
                test_layer = layer_name.rsplit("_", 1)[-1]
                layer_settings = layer_info['layer_settings']
                synth_data_helper.initiate_synthetic_data_gen(test_layer, self.wh_client, layer_settings)

            load_strategy = layer_helper.load_strategy
            commands = builder_helper.get_builder_commands_for_data_verification(layer_info, load_strategy)

            self.results.setdefault(table_name, {}).setdefault(layer_name, {})
            layer_results = self.results[table_name][layer_name]

            for command in commands:
                command.run_verification(self.wh_client, layer_name, layer_info, layer_results)

            data_clr_helper.check_and_run_test_data_clean_up(
                self.wh_client, self.src_client, self.ext_db_client,
                layer_name, layer_info, self.src_layer_settings, self.data_gen_status)

            # in the future we can pass the results to assertion module, and use it in reporting as well
            # so once we have reporting and notification capabilities we can share detailed reports
            # via email(SMTP, SES) or teams, or slack etc. (the best place would be pytest session finish hook)
            self.wh_strat_helper.process_verification_results(self.results, layer_name)

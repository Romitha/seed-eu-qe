from .context import Context
from .processing_strategy.dataframe_strategy import DataFrameStrategy
from .processing_strategy.warehouse_strategy import WarehouseStrategy


class DataProcessor:
    """
    DataProcessor is responsible for orchestrating data processing by selecting the appropriate strategy
    (DataFrame-based or Warehouse-based) based on the provided configuration

    Attributes:
        config (dict): Configuration containing data processing information
        context (Context): Context object that holds the selected strategy
    """

    def __init__(self, config):
        """
        Initialize the DataProcessor with a configuration and select the appropriate processing strategy

        Args:
            config (dict): Config dict that contains details such as table name & data processing mode
        """
        self.config = config
        self.context = Context(self._get_strategy(config))

    def _get_strategy(self, config):
        """
        Determine which data processing strategy to use based on the configuration

        Args:
            config (dict): Configuration with table and processing mode information

        Returns:
            Strategy: Selected strategy (DataFrameStrategy or WarehouseStrategy)

        Raises:
            ValueError: If an unknown data processing mode is provided in the configuration
        """
        table_name = config.get("table_name")
        mode = config[table_name]['test_info']['data_processing_mode']

        if mode == 'df_based':
            return DataFrameStrategy(self.config)
        elif mode == 'sql_based':
            return WarehouseStrategy(self.config)
        else:
            raise ValueError(f"Unknown data processing mode: {mode}")

    def process(self):
        """
        Execute the selected strategy by invoking the context's execute method

        Returns:
            Any: Result of executing the selected strategy
        """
        return self.context.execute_strategy()

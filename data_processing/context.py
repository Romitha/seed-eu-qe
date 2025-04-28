from data_processing.processing_strategy.strategy_interface import Strategy


class Context:
    """
    Context class that allows setting and executing different data processing strategies

    Attributes:
        _strategy (Strategy): The current data processing strategy
    """

    def __init__(self, strategy: Strategy):
        """
        Initialize the context with the given strategy

        Args:
            strategy (Strategy): The initial strategy to use for data processing
        """
        self._strategy = strategy

    def set_strategy(self, strategy: Strategy):
        """
        Set a new strategy for the context

        Args:
            strategy (Strategy): The new strategy to be set
        """
        self._strategy = strategy

    def execute_strategy(self):
        """
        Execute the currently set strategy

        Returns:
            Any: The result of the strategy's execution
        """
        return self._strategy.execute()

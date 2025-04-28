from abc import ABC, abstractmethod


class Strategy(ABC):
    """
    Strategy is an abstract base class that defines the interface for different strategies
    Each subclass must implement the 'execute' method to perform a specific action

    Responsibilities:
    - Enforce a standard interface for all strategy implementations
    - Allow different strategies to implement their own execution logic while adhering to a common structure
    """

    @abstractmethod
    def execute(self) -> None:
        """
        Execute the strategy's logic. Must be implemented by subclasses
        Defines the specific behavior of the strategy

        Raises:
            NotImplementedError: If the method is not implemented by the subclass
        """
        pass

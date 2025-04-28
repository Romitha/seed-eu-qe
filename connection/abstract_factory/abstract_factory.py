from abc import ABC, abstractmethod

from connection.abstract_factory.abstract_connection import AbstractConnection


class AbstractFactory(ABC):
    """
    AbstractFactory defines the interface for a factory responsible for creating connections
    This class uses the Factory Method pattern to allow the creation of different types of connections
    """

    @abstractmethod
    def create_connection(self, **kwargs: dict) -> AbstractConnection:
        """
        Create a connection instance.

        Args:
            **kwargs: Additional parameters required for creating the connection
            These can vary depending on the specific implementation (e.g. database name, configuration settings)

        Returns:
            AbstractConnection: An instance of a class that implements the AbstractConnection interface
        """
        pass

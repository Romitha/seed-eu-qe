from abc import ABC, abstractmethod


class AbstractConnection(ABC):
    """
    AbstractConnection defines the interface for database connections
    It specifies the methods required to establish and terminate a connection
    """

    @abstractmethod
    def connect(self) -> object:
        """
        Establish a connection to the database or service

        Returns:
            object: The connection object. The type of the object may vary depending on the implementation
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect from the database or service and release any resources

        This method should ensure proper cleanup of any resources used during the connection
        """
        pass

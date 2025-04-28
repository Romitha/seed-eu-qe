from abc import ABC, abstractmethod


class CloudServiceConnection(ABC):
    """
    Abstract base class defining the interface for cloud service connections
    """

    @abstractmethod
    def connect(self) -> object:
        """
        Establish a connection to the cloud service

        Returns:
            object: The connection object, which may vary depending on the cloud provider
        """
        pass

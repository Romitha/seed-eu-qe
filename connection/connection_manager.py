from connection.abstract_factory.alchemy_db_factory.alchemy_db_connection import \
    SQLAlchemyConnection
from connection.abstract_factory.alchemy_db_factory.alchemy_db_factory import \
    SQLAlchemyFactory
from connection.abstract_factory.cloud_services_factory.cloud_services_factory import \
    CloudServiceFactory
from connection.abstract_factory.remote_config_factory.remote_config_connection import \
    RemoteConfigConnection
from connection.abstract_factory.remote_config_factory.remote_config_factory import \
    RemoteConfigFactory


class ConnectionManager:
    def __init__(self) -> None:
        """
        Initialize the ConnectionManager with supported factories
        """
        self.factories = {
            'sqlalchemy_db': SQLAlchemyFactory(),
            'remote_config': RemoteConfigFactory(),
            'cloud_service': CloudServiceFactory(),
        }

    def get_connection(self, connection_type: str, **kwargs: dict) \
            -> SQLAlchemyConnection | object | RemoteConfigConnection:
        """
        Retrieve a connection from the appropriate factory

        Args:
            connection_type (str): The type of connection requested (e.g. 'sqlalchemy_db', 'remote_config')
            **kwargs (dict): Additional parameters to pass to the factory's create_connection method

        Returns:
            object: The created connection object

        Raises:
            ValueError: If the connection_type is not supported
        """
        # Split the connection_type at the second-to-last underscore
        parts = connection_type.rsplit('_', 2)
        service_type = parts[0]
        specific_type = "_".join(parts[1:])

        # Check if the service_type is supported
        if specific_type not in self.factories:
            raise ValueError(f"Unsupported connection type: {connection_type}")

        # Get the factory for the service_type
        factory = self.factories[specific_type]

        # Call create_connection with the specific_type and any additional kwargs
        return factory.create_connection(service_type, **kwargs)

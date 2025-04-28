from connection.connection_manager import ConnectionManager
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class ConnectionHelper:
    def __init__(self):
        self.connection_mgr = ConnectionManager()

    def get_connected_data_wh_client(self, connection_system, config):
        """
        Attempts to connect to a wh database using the provided connection manager and system

        Returns:
            The wh_client connection if successful None otherwise
        """
        try:
            wh_connection = self.connection_mgr.get_connection(connection_system, config=config)
            wh_client = wh_connection.connect()
            return wh_client
        except ConnectionError as e:
            LOGGER.error(f"Failed to connect to {connection_system}: {str(e)}")
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred: {str(e)}")
        return None

    def get_connected_src_storage_client(self, src_settings, config):
        """
        Attempts to connect to a cloud service using the provided connection manager and system

        Returns:
            The src_storage_client connection if successful None otherwise
        """

        cloud_service = f"{src_settings.get('storage_service')}_cloud_service"
        try:
            src_storage_connection = self.connection_mgr.get_connection(cloud_service, config=config)
            src_client = src_storage_connection.connect()
            return src_client
        except ConnectionError as e:
            LOGGER.error(f"Failed to connect to {cloud_service}: {str(e)}")
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred: {str(e)}")
        return None

    def get_connected_ext_db_client(self, src_settings, config):
        """
        Attempts to connect to an external db service using the provided connection manager and system

        Returns:
            The ext_db_client connection if successful None otherwise
        """

        cloud_service = f"{src_settings.get('external_catalog_service')}_cloud_service"
        try:
            src_storage_connection = self.connection_mgr.get_connection(cloud_service, config=config)
            ext_db_client = src_storage_connection.connect()
            return ext_db_client
        except ConnectionError as e:
            LOGGER.error(f"Failed to connect to {cloud_service}: {str(e)}")
        except Exception as e:
            LOGGER.error(f"An unexpected error occurred: {str(e)}")
        return None

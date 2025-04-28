import json

from connection.abstract_factory.remote_config_factory.remote_config_connection import \
    RemoteConfigConnection
from utils.common.aws_util import get_parameter_store_client


class ParameterStoreConnection(RemoteConfigConnection):
    def __init__(self, setting_path: str) -> None:
        """
        Initialize the ParameterStoreConnection with the parameter name to retrieve from AWSParameter Store.

        Args:
            setting_path (str): The name of the Parameter Store
        """
        self.setting_path = setting_path
        self.client = get_parameter_store_client()

    def connect(self):
        pass

    def load_settings(self) -> dict[str, any]:

        generic_settings = self.client.get_parameter(Name=f"/{self.setting_path}", WithDecryption=True)

        try:
            generic_settings_dict = json.loads(generic_settings["Parameter"]["Value"])
        except json.JSONDecodeError:
            generic_settings_dict = {"generic_settings": self.setting_path}

        return generic_settings_dict

    def disconnect(self) -> None:
        pass

    def load_secrets(self) -> dict[str, any]:
        pass

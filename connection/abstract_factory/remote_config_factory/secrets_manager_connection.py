import json

from connection.abstract_factory.remote_config_factory.remote_config_connection import \
    RemoteConfigConnection
from utils.common.aws_util import get_secrets_manager_client


class SecretsManagerConnection(RemoteConfigConnection):
    def __init__(self, secret_name: str) -> None:
        """
        Initialize the SecretsManagerConnection with the secret name to retrieve from AWS Secrets Manager

        Args:
            secret_name (str): The name of the secret in AWS Secrets Manager
        """
        self.secret_path = secret_name
        self.client = get_secrets_manager_client()

    def connect(self):
        pass

    def disconnect(self):
        pass

    def load_secrets(self) -> dict[str, any]:

        response = self.client.get_secret_value(SecretId=self.secret_path)
        secret_string = response['SecretString']

        try:
            secret_dict = json.loads(secret_string)
        except json.JSONDecodeError:
            secret_dict = {"raw_secret": secret_string}

        return secret_dict

    def load_settings(self) -> dict[str, any]:
        pass

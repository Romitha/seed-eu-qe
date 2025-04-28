from connection.abstract_factory.remote_config_factory.remote_config_connection import \
    RemoteConfigConnection
from utils.common.vault_util import load_secrets_from_vault


class VaultConnection(RemoteConfigConnection):
    def __init__(self, vault_url: str, token: str, secret_path: str) -> None:
        """
        Initialize the VaultConnection with Vault URL, token, and secret path

        Args:
            vault_url (str): The URL of the Vault instance
            token (str): The authentication token for the Vault
            secret_path (str): The path where secrets are stored in the Vault
        """
        self.vault_url = vault_url
        self.token = token
        self.secret_path = secret_path

    def connect(self) -> dict[str, any]:
        """
        Establish a connection to the Vault and load secrets

        Returns:
            dict[str, any]: A dictionary of secrets loaded from the Vault
        """
        return load_secrets_from_vault(self.vault_url, self.token, self.secret_path)

    def disconnect(self) -> None:
        """
        Disconnect from the Vault. This may not be necessary for remote configuration
        """
        pass

    def load_secrets(self) -> dict[str, any]:
        """
        Load secrets from the Vault

        Returns:
            dict[str, any]: A dictionary of secrets loaded from the Vault
        """
        return load_secrets_from_vault(self.vault_url, self.token, self.secret_path)

    def load_settings(self) -> dict[str, any]:
        """
        Load settings from the Vault. Same as loading secrets

        Returns:
            dict[str, any]: A dictionary of settings loaded from the Vault
        """
        return load_secrets_from_vault(self.vault_url, self.token, self.secret_path)

from connection.abstract_factory.abstract_factory import AbstractFactory
from connection.abstract_factory.remote_config_factory.parameter_store_connection import \
    ParameterStoreConnection
from connection.abstract_factory.remote_config_factory.remote_config_connection import \
    RemoteConfigConnection
from connection.abstract_factory.remote_config_factory.secrets_manager_connection import \
    SecretsManagerConnection
from connection.abstract_factory.remote_config_factory.vault_connection import \
    VaultConnection


class RemoteConfigFactory(AbstractFactory):
    def create_connection(self, connection_src_name: str, **kwargs) -> RemoteConfigConnection:
        if connection_src_name == 'hashi_vault':
            vault_url = kwargs.get('vault_url')
            token = kwargs.get('vault_token')
            secret_path = kwargs.get('secret_path')

            if not isinstance(vault_url, str) or not isinstance(token, str) or not isinstance(secret_path, str):
                raise ValueError("vault_url, vault_token, and secret_path must be provided as strings.")

            return VaultConnection(vault_url, token, secret_path)

        elif connection_src_name == 'secrets_manager':
            secret_path = kwargs.get('secret_path')

            if not isinstance(secret_path, str):
                raise ValueError("secret_path must be provided as a string.")

            return SecretsManagerConnection(secret_path)

        elif connection_src_name == 'parameter_store':
            setting_path = kwargs.get('setting_path')

            if not isinstance(setting_path, str):
                raise ValueError("setting_path must be provided as a string.")

            return ParameterStoreConnection(setting_path)

        else:
            raise ValueError(f"Unsupported remote config source: {connection_src_name}")

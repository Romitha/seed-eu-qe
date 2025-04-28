import hvac


def load_secrets_from_vault(vault_url: str, token: str, secret_path: str) -> dict[str, any]:
    """
    Retrieve secrets from HashiCorp Vault

    Args:
        vault_url (str): URL of the Vault server
        token (str): Authentication token for Vault
        secret_path (str): Path of the secret in Vault

    Returns:
        dict[str, any]: The secrets retrieved from Vault

    Raises:
        hvac.exceptions.VaultError: If there is an error communicating with Vault

    Examples:
        >>> load_secrets_from_vault("http://vault.example.com", "my-token", "secret/data/app")
        {'db_user': 'admin', 'db_password': 'securepassword'}
    """
    client = hvac.Client(url=vault_url, token=token)
    secret = client.secrets.kv.v2.read_secret_version(path=secret_path)
    return secret["data"]["data"]


def save_secrets_to_vault(vault_url: str, token: str, secret_path: str, secrets: dict[str, any]) -> None:
    """
    Save secrets to HashiCorp Vault

    Args:
        vault_url (str): URL of the Vault server
        token (str): Authentication token for Vault
        secret_path (str): Path of the secret in Vault
        secrets (dict[str, any]): Dictionary of secrets to be stored in Vault

    Raises:
        hvac.exceptions.VaultError: If there is an error communicating with Vault

    Examples:
        >>> save_secrets_to_vault("http://vault.example.com", "my-token", "secret/data/app",
        ...                        {"db_user": "admin", "db_password": "securepassword"})
    """
    client = hvac.Client(url=vault_url, token=token)
    client.secrets.kv.v2.create_or_update_secret(path=secret_path, secret=secrets)

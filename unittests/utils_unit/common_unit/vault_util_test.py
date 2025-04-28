import unittest
from unittest.mock import patch

from utils.common.vault_util import (load_secrets_from_vault,
                                     save_secrets_to_vault)


class TestVaultUtil(unittest.TestCase):

    @patch("hvac.Client")
    def test_get_secrets_from_vault(self, mock_client):
        # Mock the Vault client and the secret retrieval
        mock_instance = mock_client.return_value
        mock_instance.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"key": "value"}}
        }

        vault_url = "http://127.0.0.1:8200"
        token = "s.myvaulttoken"
        secret_path = "secret/data/mysecret"
        secrets = load_secrets_from_vault(vault_url, token, secret_path)

        self.assertEqual(secrets, {"key": "value"})

    @patch("hvac.Client")
    def test_save_secrets_to_vault(self, mock_client):
        # Mock the Vault client and the secret saving
        mock_instance = mock_client.return_value
        mock_instance.secrets.kv.v2.create_or_update_secret.return_value = None

        vault_url = "http://127.0.0.1:8200"
        token = "s.myvaulttoken"
        secret_path = "secret/data/mysecret"
        secrets = {"key": "value"}

        save_secrets_to_vault(vault_url, token, secret_path, secrets)

        mock_instance.secrets.kv.v2.create_or_update_secret.assert_called_with(
            path=secret_path, secret=secrets
        )

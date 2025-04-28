import os
import unittest
from pathlib import Path
from unittest.mock import patch

from utils.framework.custom_conf_util import (
    apply_overlay_to_default_yaml, get_remote_secret_config_params,
    get_remote_settings_config_params, load_env_vars, load_layered_settings,
    recursive_merge, merge_test_scope, merge_trigger
)


class TestCustomConfUtil(unittest.TestCase):

    def setUp(self):
        self.file_path = Path("/fake/path/config.json")
        self.environment = "dev"
        self.init_params = {"remote_secrets_src_type": "hashi_vault_remote_config"}
        self.env_vars = {"HOST_IP": "127.0.0.1", "KEY": "secret-key"}
        self.remote_path = "secrets/data/path"
        self.default_yaml = {
            "aws_redshift_sqlalchemy_db": {"host": "default_host"},
            "columns_info": {"id": "int"},
            "synthetic_data": {"enabled": True},
            "scd_info": {"type": "scd1"},
            "test_scope": {"a": 1, "b": [1, 2]},
            "test_info": {"type": "basic"},
            "trigger_counter": 10
        }
        self.table_yaml = {
            "aws_redshift_sqlalchemy_db": {"host": "table_host"},
            "columns_info": {"id": "bigint"},
            "synthetic_data": {"enabled": False},
            "scd_info": {"type": "scd2"},
            "test_scope": {"a": 2, "b": [1]},
            "test_info": {"type": "custom"},
            "trigger_counter": 99
        }

    @patch("utils.framework.custom_conf_util.load_json_file_in_path")
    def test_load_layered_settings_valid(self, mock_load_json):
        """Test loading layered settings with valid environment"""
        mock_load_json.return_value = {"dev": {"key1": "value1"}}
        result = load_layered_settings(self.file_path, self.environment)
        self.assertEqual(result, {"key1": "value1"})

    @patch("utils.framework.custom_conf_util.load_json_file_in_path")
    def test_load_layered_settings_missing_env(self, mock_load_json):
        """Test loading settings when environment is missing"""
        mock_load_json.return_value = {"prod": {"key2": "value2"}}
        result = load_layered_settings(self.file_path, self.environment)
        self.assertEqual(result, {})

    def test_get_remote_secret_config_params_vault(self):
        """Test fetching Vault-based remote secret config"""
        result = get_remote_secret_config_params(self.init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {
            "vault_url": "127.0.0.1",
            "vault_token": "secret-key",
            "secret_path": "secrets/data/path"
        })

    def test_get_remote_secret_config_params_secrets_manager(self):
        """Test fetching Secrets Manager-based remote secret config"""
        self.init_params["remote_secrets_src_type"] = "secrets_manager_remote_config"
        result = get_remote_secret_config_params(self.init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {"secret_path": "secrets/data/path"})

    def test_get_remote_secret_config_params_invalid_type(self):
        """Test fetching remote secret config with invalid type"""
        self.init_params["remote_secrets_src_type"] = "unknown_source"
        result = get_remote_secret_config_params(self.init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {})

    def test_get_remote_settings_config_params_vault(self):
        """Test fetching Vault-based remote settings config"""
        init_params = {"remote_settings_src_type": "hashi_vault_remote_config"}
        result = get_remote_settings_config_params(init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {
            "vault_url": "127.0.0.1",
            "vault_token": "secret-key",
            "setting_path": "secrets/data/path"
        })

    def test_get_remote_settings_config_params_parameter_store(self):
        """Test fetching Parameter Store-based remote settings config"""
        init_params = {"remote_settings_src_type": "parameter_store_remote_config"}
        result = get_remote_settings_config_params(init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {"setting_path": "secrets/data/path"})

    def test_get_remote_settings_config_params_invalid_type(self):
        """Test fetching remote settings config with invalid type"""
        init_params = {"remote_settings_src_type": "unknown_source"}
        result = get_remote_settings_config_params(init_params, self.env_vars, self.remote_path)
        self.assertEqual(result, {})

    # Test: load_env_vars
    @patch.dict(os.environ, {"CONF_DB_HOST": "localhost", "CONF_DB_PORT": "5432", "UNRELATED_VAR": "ignore"})
    def test_load_env_vars(self):
        """Test loading environment variables with a specific prefix"""
        result = load_env_vars("CONF_")
        self.assertEqual(result, {"CONF_DB_HOST": "localhost", "CONF_DB_PORT": "5432"})

    # Test: apply_overlay_to_default_yaml
    @patch("utils.framework.custom_conf_util.merge_data_platform_node", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_column_info_node", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_synthetic_data", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_scd_settings", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_test_scope", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_test_info", side_effect=lambda d, t: t)
    @patch("utils.framework.custom_conf_util.merge_trigger", side_effect=lambda d, t: t)
    def test_apply_overlay_to_default_yaml(self, *mocks):
        result = apply_overlay_to_default_yaml(self.default_yaml, self.table_yaml)
        self.assertEqual(result["columns_info"], {"id": "bigint"})
        self.assertEqual(result["trigger_counter"], 99)

    # Test: recursive_merge
    def test_recursive_merge_valid(self):
        """Test recursive merging of two dictionaries"""
        result = recursive_merge({"a": {"b": 1}}, {"a": {"b": 2}})
        self.assertEqual(result, {"a": {"b": 2}})

    def test_recursive_merge_new_keys(self):
        """Test recursive merge when new keys are introduced"""
        with self.assertRaises(ValueError) as context:
            recursive_merge({"a": {"b": 1}}, {"a": {"b": 2}, "new_key": 3})
        self.assertIn("New key 'new_key'", str(context.exception))

    def test_merge_test_scope_diff_and_null(self):
        default = {"a": 1, "b": [1, 2], "c": {"x": 10}, "d": None}
        table = {"a": 2, "b": [1], "c": {"x": 10}, "d": None}
        result = merge_test_scope(default, table)
        self.assertEqual(result["a"], 2)  # different primitive
        self.assertEqual(result["b"], [2])  # partial list diff
        self.assertEqual(result["c"], {"x": None})  # matching nested dict → kept structure with None
        self.assertIsNone(result["d"])  # identical None → stays None

    def test_merge_test_scope_with_extra_keys_raises(self):
        default = {"a": 1}
        table = {"a": 1, "extra": 2}
        with self.assertRaises(KeyError):
            merge_test_scope(default, table)

    def test_merge_trigger_logic(self):
        self.assertEqual(merge_trigger(5, None), 5)
        self.assertEqual(merge_trigger(5, 10), 10)

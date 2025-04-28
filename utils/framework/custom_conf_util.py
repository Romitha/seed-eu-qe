import os
from pathlib import Path
from typing import Any, Dict

from utils.common.file_util import load_json_file_in_path


def load_layered_settings(file_path: Path, environment: str) -> Dict[str, Any]:
    """
    Load layered settings from a TOML file

    Args:
        file_path (Path): Path to the TOML file.
        environment (str): The environment layer to load (e.g. 'dev', 'stg', 'prod')

    Returns:
        Dict[str, Any]: Merged settings.
    """
    settings = load_json_file_in_path(file_path)

    env_settings = settings.get(environment, {})

    merged_settings = {**env_settings}
    return merged_settings


def get_remote_secret_config_params(init_params, env_vars, remote_path) -> Dict[str, Any]:
    """
    Get the parameters required for the remote configuration handler (Vault or Secrets Manager)

    Args:
        init_params (Dict[str, Any]): Initialization parameters from the helper class
        env_vars (Dict[str, str]): Environment variables related to the remote configuration
        remote_path: The path to the remote configuration file

    Returns:
        Dict[str, Any]: A dictionary of parameters for the remote configuration handler
    """
    return_params = {}

    if init_params.get("remote_secrets_src_type") == "hashi_vault_remote_config":
        return_params = {
            "vault_url": env_vars.get("HOST_IP"),
            "vault_token": env_vars.get("KEY"),
            "secret_path": f"{remote_path}",
        }

    elif init_params.get("remote_secrets_src_type") == "secrets_manager_remote_config":
        return_params = {
            "secret_path": f"{remote_path}",
        }

    return return_params


def get_remote_settings_config_params(init_params, env_vars, remote_path) -> Dict[str, Any]:

    return_params = {}

    if init_params.get("remote_settings_src_type") == "hashi_vault_remote_config":
        return_params = {
            "vault_url": env_vars.get("HOST_IP"),
            "vault_token": env_vars.get("KEY"),
            "setting_path": f"{remote_path}",
        }

    elif init_params.get("remote_settings_src_type") == "parameter_store_remote_config":
        return_params = {
            "setting_path": f"{remote_path}",
        }

    return return_params


def load_env_vars(prefix: str = "CONF_") -> Dict[str, str]:
    """
    Load settings from environment variables prefixed with the provided prefix

    Args:
        prefix (str): The prefix to filter environment variables (default is 'CONF_')

    Returns:
        Dict[str, str]: A dictionary of settings loaded from environment variables
    """
    return {key: value for key, value in os.environ.items() if key.startswith(prefix)}


def apply_overlay_to_default_yaml(default_yaml: Dict[str, Any], table_yaml: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply overlay logic to YAML configurations using pure helper functions

    Args:
        default_yaml (dict): Default YAML structure (whitelist)
        table_yaml (dict): Table-specific YAML structure (blacklist)

    Returns:
        dict: Fully merged configurations
    """
    return {
        "aws_redshift_sqlalchemy_db": merge_data_platform_node(
            default_yaml.get("aws_redshift_sqlalchemy_db", {}),
            table_yaml.get("aws_redshift_sqlalchemy_db", {})
        ),
        "columns_info": merge_column_info_node(
            default_yaml.get("columns_info", {}),
            table_yaml.get("columns_info", {})
        ),
        "synthetic_data": merge_synthetic_data(
            default_yaml.get("synthetic_data", {}),
            table_yaml.get("synthetic_data", {})
        ),
        "scd_info": merge_scd_settings(
            default_yaml.get("scd_info", {}),
            table_yaml.get("scd_info", {})
        ),
        "test_scope": merge_test_scope(
            default_yaml.get("test_scope", {}),
            table_yaml.get("test_scope", {})
        ),
        "test_info": merge_test_info(
            default_yaml.get("test_info", {}),
            table_yaml.get("test_info", {})
        ),
        "trigger_counter": merge_trigger(
            default_yaml.get("trigger_counter"),
            table_yaml.get("trigger_counter")
        )
    }


def merge_data_platform_node(default_node: Dict[str, Any], table_node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'aws_redshift_sqlalchemy_db' nodes
    """
    return recursive_merge(default_node, table_node)


def merge_column_info_node(default_node: Dict[str, Any], table_node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'columns_info' nodes
    """
    return recursive_merge(default_node, table_node)


def merge_synthetic_data(default_node: Dict[str, Any], table_node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'synthetic_data' nodes
    """
    return recursive_merge(default_node, table_node)


def merge_scd_settings(default_node: Dict[str, Any], table_node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'scd_info' nodes
    """
    return recursive_merge(default_node, table_node)


def merge_test_scope(default_scope: Dict[str, Any], table_scope: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'test_scope' node by removing matching values and retaining structure

    Args:
        default_scope (dict): Default test scope configuration
        table_scope (dict): Table-specific test scope configuration

    Returns:
        dict: Merged test scope configuration with non-matching values or null
    """
    extra_keys = table_scope.keys() - default_scope.keys()
    if extra_keys:
        raise KeyError(f"Unexpected keys in table_scope: {extra_keys}")

    merged_scope = {}

    for key, default_value in default_scope.items():
        table_value = table_scope.get(key)

        if isinstance(default_value, dict) and isinstance(table_value, dict):
            merged_scope[key] = merge_test_scope(default_value, table_value)
        elif isinstance(default_value, list) and isinstance(table_value, list):
            diff = [val for val in default_value if val not in table_value]
            merged_scope[key] = diff if diff else None
        elif table_value is None:
            merged_scope[key] = default_value
        else:
            merged_scope[key] = None if default_value == table_value else table_value

    return merged_scope


def merge_test_info(default_node: Dict[str, Any], table_node: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge 'test_info' nodes
    """
    return recursive_merge(default_node, table_node)


def merge_trigger(default_value: Any, table_value: Any) -> Any:
    """
    Merge 'trigger_counter' node by prioritizing table value if present

    Args:
        default_value (any): Default value for the trigger node
        table_value (any): Table-specific value for the trigger node

    Returns:
        any: Merged trigger value
    """
    return table_value if table_value is not None else default_value


def recursive_merge(default_dict: Dict[str, Any], table_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge two dictionaries, raising an error for unknown keys in table_dict
    """
    merged = {}

    for key, default_value in default_dict.items():
        if key in table_dict:
            table_value = table_dict[key]
            if isinstance(default_value, dict) and isinstance(table_value, dict):
                merged[key] = recursive_merge(default_value, table_value)
            else:
                merged[key] = table_value
        else:
            merged[key] = default_value

    for key in table_dict:
        if key not in default_dict:
            raise ValueError(f"New key '{key}' has been configured that is not in the default template config")

    return merged

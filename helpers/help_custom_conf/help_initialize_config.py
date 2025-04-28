import logging
from threading import Lock
from typing import Any, Dict

from connection.connection_manager import ConnectionManager
from custom_conf.conf_manager import ConfManager
from utils.framework import custom_path_util
from utils.framework.custom_conf_util import (
    get_remote_secret_config_params, get_remote_settings_config_params,
    load_env_vars)
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


class HelpInitializeConfig:
    def __init__(
        self,
        conf_manager: ConfManager,
        team_key: str,
        environment: str,
        params: Dict[str, Any],
    ):
        self.conf_manager = conf_manager
        self.team_key = team_key
        self.environment = environment
        self.params = params
        self.team_dir_path = custom_path_util.get_teams_root_folder_path()
        self.lock = Lock()
        self.connection_manager = ConnectionManager()

    def initialize(self) -> ConfManager:

        if self.params.get("detect_env_vars"):
            self._load_env_vars()

        self._load_remote_secrets()
        self._load_remote_settings()

        return self.conf_manager

    def _load_env_vars(self) -> None:
        env_vars = load_env_vars()
        self.conf_manager.load(env_vars)

    def _load_remote_secrets(self) -> None:
        with self.lock:
            env_vars = self._extract_env_vars_for_service()
            remote_conf_path = f"{self.team_key}/{self.environment}/secrets"
            remote_config_params = get_remote_secret_config_params(
                self._get_init_params(), env_vars, remote_conf_path
            )
            connection = self.connection_manager.get_connection(
                self.params["remote_secrets_src_type"],
                **remote_config_params,
            )

            # Load secrets from remote (Vault or Secrets Manager)
            secrets_data = connection.load_secrets()
            LOGGER.debug(f"Loaded secrets data: {secrets_data}")

            # Load secrets directly into conf_manager
            self.conf_manager.load(secrets_data)

    def _load_remote_settings(self) -> None:
        with self.lock:
            env_vars = self._extract_env_vars_for_service()
            # in the future, if we are not dynamically able to fetch these names, then
            # we need to move these static strings into .ini file
            remote_conf_path = f"{self.team_key.upper()}/{self.environment.upper()}/SETTINGS"
            remote_config_params = get_remote_settings_config_params(
                self._get_init_params(), env_vars, remote_conf_path
            )
            connection = self.connection_manager.get_connection(
                self.params["remote_settings_src_type"],
                **remote_config_params,
            )

            settings_data = connection.load_settings()
            LOGGER.debug(f"Loaded remote data: {settings_data}")

            self.conf_manager.load(settings_data)

    def _extract_env_vars_for_service(self) -> dict[str, str]:
        """
        Extracts environment variables for the service based on a predefined prefix pattern.

        Returns:
            dict[str, str]: A dictionary of environment variables with the prefix stripped,
            or an empty dictionary if environment variable detection is disabled
        """
        # Return an empty dictionary if environment variable detection is disabled
        if not self.params.get('detect_env_vars', False):
            LOGGER.debug("Environment variable detection is disabled")
            return {}

        # Construct the prefix for filtering environment variables
        remote_secrets_src_type = '_'.join(self.params['remote_secrets_src_type'].split('_')[:2])
        prefix = f"CONF_{self.team_key.upper()}_{self.environment.upper()}_{remote_secrets_src_type.upper()}_"

        # Load all environment variables
        all_env_vars = load_env_vars()

        # Filter environment variables based on the prefix
        matching_env_vars = {
            key: value for key, value in all_env_vars.items() if key.startswith(prefix)
        }

        # Strip the prefix from the keys
        stripped_env_vars = {
            key[len(prefix):]: value for key, value in matching_env_vars.items()
        }

        return stripped_env_vars

    def _get_init_params(self) -> Dict[str, Any]:
        return {
            "team_key": self.team_key,
            "environment": self.environment,
            **self.params,
        }

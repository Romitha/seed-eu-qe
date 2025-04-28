import logging

from custom_conf.conf_manager import ConfManager
from helpers.help_custom_conf.help_initialize_config import \
    HelpInitializeConfig

LOGGER = logging.getLogger(__name__)


class ConfigInitializer:
    def __init__(
            self,
            team_key: str,
            environment: str,
            detect_env_vars: bool | None = None,
            remote_secrets_src_type: str | None = None,
            remote_settings_src_type: str | None = None
            # same applied here as well
    ) -> None:
        """
        Initialize the ConfigInitializer with team key, environment, and mandatory parameters
        to control environment detection and secret pulling.

        Args:
            team_key (str): The unique identifier of the team
            environment (str): The environment (example 'dev', 'prod')
            detect_env_vars (bool | None): Flag to enable environment variable detection
            remote_secrets_src_type (str | None): The type of remote configuration source
            remote_settings_src_type (str | None): The type of remote setting source
        """
        # Store parameters in a dictionary to be passed to the helper class
        self.params = {
            "detect_env_vars": detect_env_vars,
            "remote_secrets_src_type": remote_secrets_src_type,
            "remote_settings_src_type": remote_settings_src_type,
            # please copy and update here
        }
        # Initialize the team key and environment for configuration
        self.team_key = team_key
        self.environment = environment

        # Create an instance of ConfManager to manage configuration settings
        self.conf_manager = ConfManager()

    def initialize(self) -> ConfManager:
        """
        Initialize the configuration using the HelpInitializeConfig helper class
        The helper handles loading environment variables, pulling remote secrets, and
        other configuration setup based on the team and environment

        Returns:
            ConfManager: The ConfManager instance with loaded configuration settings
        """
        # Create an instance of the helper class
        helper = HelpInitializeConfig(self.conf_manager, self.team_key, self.environment, self.params)

        # Trigger the initialization process and return the fully initialized configuration manager
        return helper.initialize()

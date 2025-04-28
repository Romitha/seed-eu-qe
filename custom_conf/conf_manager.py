from threading import Lock

from helpers.help_custom_conf.help_conf_manager import HelpConfigManager

helper = HelpConfigManager()


class ConfManager:
    """
    ConfManager is responsible for managing configuration settings in a thread-safe way
    It allows loading, retrieving, updating, and clearing configuration settings
    """

    def __init__(self) -> None:
        """
        Initialize the ConfManager with an empty settings dictionary and a lock for thread safety
        """
        self.settings: dict[str, any] = {}  # Holds the configuration settings
        self._lock = Lock()  # Ensures thread-safe access to the settings

    def load(self, loader: any, environment: str | None = None) -> None:
        """
        Load new settings into the configuration manager. If an environment is specified
        it loads the settings into that environment's specific settings dictionary

        Args:
            loader (any): A dictionary or an object with a .load() method that provides the settings
            environment (str | None): The name of the environment (example 'dev', 'prod')
        """
        with self._lock:  # Ensure thread-safe access to the settings
            self.settings = helper.get_merged_settings(loader, environment, self.settings)

    def get_settings(self, key: str, default: any = None) -> any:
        """
        Retrieve a setting by key. If the key doesn't exist, return the default value (None by default)

        Args:
            key (str): The key of the setting to retrieve
            default (any): The default value to return if the key is not found

        Returns:
            any: The value of the setting or the default value if the key is not found
        """
        with self._lock:  # Ensure thread-safe access to the settings
            return self.settings.get(key, default)

    def set_settings(self, key: str, value: any) -> None:
        """
        Set a new key-value pair in the settings

        Args:
            key (str): The key of the setting to add or update
            value (any): The value of the setting to store
        """
        with self._lock:  # Ensure thread-safe access to the settings
            self.settings[key] = value

    def clear(self) -> None:
        """
        Clear all settings from the configuration manager
        """
        with self._lock:  # Ensure thread-safe access to the settings
            self.settings.clear()  # Clear the entire settings dictionary

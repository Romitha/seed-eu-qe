from utils.common.dict_util import merge_dicts


class HelpConfigManager:
    def __init__(self):
        """
        Initialization can take place in future developments
        """

    @staticmethod
    def get_merged_settings(loader, environment, settings):

        # Determine if loader is a dict or an object with .load() method
        if isinstance(loader, dict):
            new_settings = loader
        else:
            new_settings = loader.load()

        # If an environment is specified then load the settings into that environment
        if environment:
            if environment not in settings:
                settings[environment] = {}  # Create a new dict for the environment if it doesn't exist
            settings[environment] = merge_dicts(
                settings[environment], new_settings  # Merge new settings with existing settings
            )

            return settings
        else:
            # If no environment is specified, merge settings globally
            settings = merge_dicts(settings, new_settings)
            return settings

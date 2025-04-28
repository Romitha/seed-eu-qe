from collections import defaultdict

from utils.common.file_util import load_json_file_in_path
from utils.framework.custom_logger_util import get_logger

LOGGER = get_logger()


def process_file_names(file_names):
    """
    Process a list of file names, organizing them by team and core name
    Only include files starting with 'data-checks/' and ending with '.yaml'
    The team name is extracted from the path segment before the file name

    Args:
        file_names: A list of file paths

    Returns:
        A nested dictionary where the first level is the team, the second is the core file name
    """
    nested_dict = defaultdict(lambda: defaultdict(list))

    for file_path in file_names:
        # Only process files that start with 'data-checks/' and end with '.yaml'
        file_path = "".join(file_path.split())
        if not file_path.startswith("data-checks/") or not file_path.endswith(".yaml"):
            continue

        parts = file_path.split('/')

        # Check if the first part is "data-checks" and the last part ends with ".yaml"
        if parts[0] == "data-checks" and parts[-1].endswith(".yaml"):
            # Extract team name from elements between first and last
            team_key = "_".join(parts[1:-1])

            # The core file name without the '.yaml' extension
            file_name = parts[-1]
            file_name_no_ext = file_name.replace('.yaml', '')

            # Add the file to the nested dictionary under team_name and file_name_no_ext
            nested_dict[team_key][file_name_no_ext] = []

    return {k: dict(v) for k, v in nested_dict.items()}


def load_json_configuration(json_path):
    """
    Load the configuration data from the JSON configuration file

    Args:
        json_path (str): Path to the JSON configuration file

    Returns:
        dict: The configuration data
    """
    LOGGER.info("Loading configuration from JSON file")
    config_data = load_json_file_in_path(json_path)
    return config_data


def validate_team_config(team_config):
    """
    Validate a single team's configuration

    Args:
        team_config (dict): The team's configuration

    Raises:
        ValueError: If a mandatory field is missing
    """
    mandatory_fields = {
        "run_mode": "run_mode is mandatory. Please provide a valid 'run_mode': local, cicd or etl",
        "test_environments": "Test environments are mandatory. Please provide valid 'test_environments'",
        "file_names": "File names are mandatory. Please provide a valid 'file_names'"
    }

    for field, error_message in mandatory_fields.items():
        if not team_config.get(field):
            raise ValueError(error_message)

    optional_params_log = {
        "detect_env_vars": "Optional Param - detect environment variables is Disabled",
        "remote_secrets_src_type": "Optional Param - remote secrets source is Disabled",
        "remote_settings_src_type": "Optional Param - remote secrets source is Disabled"
    }

    # Log based on optional parameters
    for param, log_message in optional_params_log.items():
        if not team_config.get(param):
            LOGGER.debug(log_message)

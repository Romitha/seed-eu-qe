from pathlib import Path

from utils.common.path_util import convert_underscore_to_nested_path


def find_project_root(start_path: Path, markers: list[str] | None = None) -> Path:
    """
    Recursively searches for the project root directory by looking for specific marker files.

    Args:
        start_path (Path): The starting path for the search.
        markers (list[str] | None): A list of marker files to identify the project root.

    Returns:
        Path: The path to the project root directory.

    Raises:
        FileNotFoundError: If the project root directory cannot be found.
    """
    if markers is None:
        markers = ["README.md", "Dockerfile", "poetry.lock"]

    current_path = start_path.resolve()

    if any((current_path / marker).exists() for marker in markers):
        return current_path

    if current_path.parent == current_path:
        raise FileNotFoundError("Project root not found.")

    return find_project_root(current_path.parent, markers)


def get_framework_root_path() -> Path:
    """
    Returns the root directory of the framework.

    Returns:
        Path: The root directory of the framework.
    """
    start_path = Path(__file__)
    return find_project_root(start_path)


def get_teams_root_folder_path() -> Path:
    """
    Returns the path to the teams root folder.

    Returns:
        Path: The path to the teams root folder.
    """
    return get_framework_root_path() / "custom_conf" / "teams"


def get_custom_conf_root_path() -> Path:
    """
    Returns the path to the custom configuration root folder.

    Returns:
        Path: The path to the custom configuration root folder.
    """
    return get_framework_root_path() / "custom_conf"


def get_team_sub_dir_path(base_path: Path, team_key: str) -> Path:
    """
    Get the folder path for a team based on the team key, considering only the first two parts of the key.

    Args:
        base_path (Path): Base path for the teams folder.
        team_key (str): Team key (e.g. 'root_subdir_pgm').

    Returns:
        Path: The path to the team's sub-directory.
    """
    sub_dir_key = "_".join(team_key.split('_')[:2])
    folder_path = convert_underscore_to_nested_path(sub_dir_key)
    return base_path / folder_path


def get_team_folder_path_with_key(base_path: Path, team_key: str) -> Path:
    """
    Get the folder path for a team based on the team key, where underscores in the key
    are translated into directory levels.

    Args:
        base_path (Path): Base path for the teams folder.
        team_key (str): Team key (e.g. 'root_subdir_pgm').

    Returns:
        Path: The full path to the team's folder.
    """
    folder_path = convert_underscore_to_nested_path(team_key)
    return base_path / folder_path


def get_cloud_data_checks_team_path(team_key: str) -> str:
    """
    Get the S3 path for the cloud table YAML configuration for a given team key.

    Args:
        team_key (str): Team key (e.g. 'root_subdir_pgm').

    Returns:
        str: S3 path for the cloud table YAML configuration.
    """
    folder_path = convert_underscore_to_nested_path(team_key)
    return f"data-checks/{folder_path}/".replace("\\", "/")


def get_cloud_data_checks_sub_team_path(team_key: str) -> str:
    """
    Get the S3 path for the cloud table YAML configuration for a given team key,
    considering only the first two parts of the key.

    Args:
        team_key (str): Team key (e.g. 'root_subdir_pgm').

    Returns:
        str: S3 path for the cloud table YAML configuration.
    """
    sub_dir_key = "_".join(team_key.split('_')[:2])
    folder_path = convert_underscore_to_nested_path(sub_dir_key)
    return f"data-checks/{folder_path}/".replace("\\", "/")

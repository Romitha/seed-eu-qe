import json
import re
from pathlib import Path
from typing import Any

import yaml


def file_exists_in_path(file_path: str | Path) -> bool:
    """
    Check if a file exists at the given path

    Args:
        file_path (str | Path): Path to the file

    Returns:
        bool: True if the file exists, False otherwise

    Raises:
        TypeError: If the input is not a string or Path object

    Examples:
        >>> file_exists_in_path("example.txt")
        False

        >>> file_exists_in_path(Path("example.txt"))
        False
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")
    return Path(file_path).is_file()


def load_json_file_in_path(file_path: str | Path) -> dict[str, any]:
    """
    Load a JSON file from the given path and return the data as a dictionary

    Args:
        file_path (str | Path): Path to the JSON file

    Returns:
        dict[str, any]: Data loaded from the JSON file

    Raises:
        TypeError: If the input is not a string or Path object
        FileNotFoundError: If the file does not exist
        ValueError: If the file cannot be parsed as JSON

    Examples:
        >>> load_json_file_in_path("config.json")
        {'key': 'value'}

        >>> load_json_file_in_path(Path("config.json"))
        {'key': 'value'}
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8") as file:
        try:
            return json.load(file)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {file_path}") from e


def load_yaml_file_in_path(file_path: str | Path) -> dict[str, Any]:
    """
    Load a YAML file from the given path and return the data as a dictionary

    Args:
        file_path (str | Path): Path to the YAML file

    Returns:
        dict[str, Any]: Data loaded from the YAML file

    Raises:
        TypeError: If the input is not a string or Path object
        FileNotFoundError: If the file does not exist
        ValueError: If the file cannot be parsed as YAML

    Examples:
        >>> load_yaml_file_in_path("config.yaml")
        {'key': 'value'}

        >>> load_yaml_file_in_path(Path("config.yaml"))
        {'key': 'value'}
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8") as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML file: {file_path}") from e


def load_multiline_sql_file_in_path(sql_content):
    # Remove SQL single-line comments (-- comment)
    sql_content = re.sub(r"--.*", "", sql_content)

    # Remove SQL multi-line comments (/* comment */)
    sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

    # Normalize whitespace (remove excess spaces and new lines)
    sql_content = re.sub(r"\s+", " ", sql_content).strip()

    # Split SQL statements correctly (handles newlines & multiple statements)
    sql_statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

    return sql_statements


def load_file_in_path(file_path: str | Path) -> dict[str, Any]:
    """
    Load a file (JSON or YAML) from the given path and return the data as a dictionary

    Args:
        file_path (str | Path): Path to the file

    Returns:
        dict[str, Any]: Data loaded from the file

    Raises:
        TypeError: If the input is not a string or Path object
        FileNotFoundError: If the file does not exist
        ValueError: If the file type is unsupported or cannot be parsed

    Examples:
        >>> load_file_in_path("config.json")
        {'key': 'value'}

        >>> load_file_in_path("config.yaml")
        {'key': 'value'}
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    match path.suffix.lower():
        case ".json":
            return load_json_file_in_path(path)
        case ".yaml" | ".yml":
            return load_yaml_file_in_path(path)
        case _:
            raise ValueError(f"Unsupported file type: {path.suffix}")


def load_multiple_files_in_path(file_paths: list[str | Path]) -> list[dict[str, Any]]:
    """
    Load multiple files (JSON or YAML) from the given paths and return a list of dictionaries

    Args:
        file_paths (list[str | Path]): Paths to the files

    Returns:
        list[dict[str, Any]]: List of dictionaries loaded from the files

    Raises:
        TypeError: If any element in file_paths is not a string or Path object
        FileNotFoundError: If any file does not exist
        ValueError: If any file type is unsupported or cannot be parsed

    Examples:
        >>> load_multiple_files_in_path(["config.json", "config.yaml"])
        [{'key': 'value'}, {'key': 'value'}]
    """
    if not isinstance(file_paths, list) or any(not isinstance(fp, (str, Path)) for fp in file_paths):
        raise TypeError("file_paths must be a list of strings or Path objects")

    return [load_file_in_path(file_path) for file_path in file_paths]

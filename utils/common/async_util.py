import asyncio
import json
from pathlib import Path
from typing import Any

import aiofiles


async def async_load_json_file_in_path(file_path: str | Path) -> dict[str, Any]:
    """
    Asynchronously load a JSON file from the given path and return the data as a dictionary

    Args:
        file_path (str | Path): Path to the JSON file

    Returns:
        dict[str, Any]: Data from the JSON file

    Raises:
        TypeError: If file_path is not a string or Path object
        FileNotFoundError: If the file does not exist
        ValueError: If the file cannot be parsed as JSON

    Examples:
        >>> await async_load_json_file_in_path("config.json")
        {'key': 'value'}

        >>> await async_load_json_file_in_path(Path("config.json"))
        {'key': 'value'}
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    async with aiofiles.open(path, "r", encoding="utf-8") as file:
        try:
            content = await file.read()
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {file_path}") from e


async def async_load_file_in_path(file_path: str | Path) -> dict[str, Any]:
    """
    Asynchronously load a file (JSON) from the given path and return the data as a dictionary

    Args:
        file_path (str | Path): Path to the file

    Returns:
        dict[str, Any]: Data loaded from the file

    Raises:
        TypeError: If file_path is not a string or Path object
        FileNotFoundError: If the file does not exist
        ValueError: If the file type is unsupported or cannot be parsed

    Examples:
        >>> await async_load_file_in_path("config.json")
        {'key': 'value'}
    """
    if not isinstance(file_path, (str, Path)):
        raise TypeError("file_path must be a string or Path object")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    match path.suffix.lower():
        case ".json":
            return await async_load_json_file_in_path(path)
        case _:
            raise ValueError(f"Unsupported file type: {path.suffix}")


async def async_load_multiple_files(file_paths: list[str | Path]) -> list[dict[str, Any]]:
    """
    Asynchronously load multiple files from the given paths and return a list of dictionaries

    Args:
        file_paths (list[str | Path]): Paths to the files

    Returns:
        list[dict[str, Any]]: List of dictionaries loaded from the files

    Raises:
        TypeError: If any element in file_paths is not a string or Path object
        FileNotFoundError: If any file does not exist
        ValueError: If any file type is unsupported or cannot be parsed

    Examples:
        >>> await async_load_multiple_files(["config1.json", "config2.json"])
        [{'key1': 'value1'}, {'key2': 'value2'}]
    """
    if not isinstance(file_paths, list) or any(not isinstance(fp, (str, Path)) for fp in file_paths):
        raise TypeError("file_paths must be a list of strings or Path objects")

    tasks = [async_load_file_in_path(file_path) for file_path in file_paths]
    return list(await asyncio.gather(*tasks))

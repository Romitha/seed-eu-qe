from pathlib import Path


def construct_path(*args: str | Path) -> Path:
    """
    Construct a path by joining multiple path components

    Args:
        *args (str | Path): Path components to join

    Returns:
        Path: Constructed path

    Raises:
        TypeError: If any argument is not a string or Path object

    Examples:
        >>> construct_path("folder", "subfolder", "file.txt")
        PosixPath('folder/subfolder/file.txt')

        >>> construct_path(Path("folder"), "subfolder")
        PosixPath('folder/subfolder')
    """
    if not args:
        raise ValueError("At least one path component must be provided")
    if any(not isinstance(arg, (str, Path)) for arg in args):
        raise TypeError("All arguments must be strings or Path objects")
    return Path(*map(str, args))


def convert_underscore_to_nested_path(value: str | Path) -> Path:
    """
    Convert an underscore-separated string or Path object to a nested folder path

    Args:
        value (str | Path): An underscore-separated string or Path object

    Returns:
        Path: Corresponding nested folder path

    Raises:
        ValueError: If the input string or Path is empty
        TypeError: If the input is not a string or Path object

    Examples:
        >>> convert_underscore_to_nested_path("folder_subfolder_file.txt")
        PosixPath('folder/subfolder/file.txt')

        >>> convert_underscore_to_nested_path(Path("folder_subfolder_file.txt"))
        PosixPath('folder/subfolder/file.txt')
    """
    if isinstance(value, str) and not value.strip():
        raise ValueError("Input value must not be an empty string")
    if isinstance(value, Path) and str(value) == ".":
        raise ValueError("Input value must not be an empty Path")
    if not isinstance(value, (str, Path)):
        raise TypeError("Input value must be a string or Path object")

    value_str = value.as_posix() if isinstance(value, Path) else value
    return Path(*value_str.split("_"))

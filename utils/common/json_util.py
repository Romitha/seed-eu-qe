import json
from typing import Any


def dump_json_data(data: Any, indent: int) -> str:
    """
    Dump a data structure into a formatted JSON string with customizable indentation

    Args:
        data (Any): Data structure to convert into a JSON string
        indent (int): Number of spaces to use for indentation

    Returns:
        str: Formatted JSON string

    Raises:
        TypeError: If data is not serializable to JSON or if indent is not an integer

    Examples:
        >>> dump_json_data({"key": "value"}, 2)
        '{\\n  "key": "value"\\n}'

        >>> dump_json_data([1, 2, 3], 4)
        '[\\n    1,\\n    2,\\n    3\\n]'
    """
    if not isinstance(indent, int):
        raise TypeError("indent must be an integer")

    return json.dumps(data, indent=indent)

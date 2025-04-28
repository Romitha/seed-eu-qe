from typing import Any


def merge_dicts(*dict_args: dict[str, Any]) -> dict[str, Any]:
    """
    Merge multiple dictionaries into one

    Args:
        *dict_args (dict[str, Any]): Any number of dictionaries to merge

    Returns:
        dict[str, Any]: The merged dictionary

    Examples:
        >>> merge_dicts({"a": 1}, {"b": 2}, {"c": 3})
        {'a': 1, 'b': 2, 'c': 3}

        >>> merge_dicts({"a": 1, "b": 2}, {"b": 3, "c": 4})
        {'a': 1, 'b': 3, 'c': 4}
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

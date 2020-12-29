from enum import Enum


class AutoName(Enum):
    """
    A base class for Enum-derived types that want to have options with identical names and values.
    See: https://docs.python.org/3/library/enum.html#using-automatic-values
    """
    def _generate_next_value_(name, start, count, last_values):
        return name


def deep_enum_to_str(d):
    """
    Replaces enums with their plain string equivalents, i.e. `str(Enum.name)`.  If a dict or list is passed to this
    function, it will be applied deeply/recursively through all nested objects in the collection.
    """
    if isinstance(d, Enum):
        return str(d.name)
    if isinstance(d, dict):
        for k, v in d.items():
            d[k] = deep_enum_to_str(v)
    if isinstance(d, list):
        for n in range(len(d)):
            d[n] = deep_enum_to_str(d[n])
    return d

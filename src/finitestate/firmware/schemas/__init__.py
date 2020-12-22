import importlib
import os
import pkgutil
import sys

from pyspark.sql.types import StructType

__all_schemas = {}

for loader, module_name, is_pkg in  pkgutil.walk_packages(__path__):
    _module = loader.find_module(module_name).load_module(module_name)
    for member_name in dir(_module):
        if 'schema' in member_name:
            __all_schemas[module_name.replace('schema_', '')] = getattr(_module, member_name)
            break


def get_schema(name: str) -> StructType:
    return __all_schemas.get(name)

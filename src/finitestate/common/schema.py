import re
import json
import fastjsonschema
import os.path

import finitestate.common.schemas

uri_pattern     = re.compile('custom://(.*)')
validator_cache = {}


def _load_referenced_schema(uri):
    return _load_schema(uri_pattern.match(uri).group(1))


def _load_schema(schema_name):
    return json.loads(finitestate.common.schemas.__loader__.get_data(os.path.join(os.path.dirname(__file__), 'schemas', f"{schema_name}.json")))


def _load_validator(schema_name):
    if schema_name in validator_cache:
        return validator_cache[schema_name]

    validator_cache[schema_name] = fastjsonschema.compile(_load_schema(schema_name), handlers = { 'custom' : _load_referenced_schema  })

    return validator_cache[schema_name]


def assert_data_matches_schema(data, schema_name):
    _load_validator(schema_name)(data)
    return data

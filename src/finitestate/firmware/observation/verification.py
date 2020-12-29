import json
from decimal import Decimal
from distutils.util import strtobool
from enum import auto, unique
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import urlsplit, urlunsplit

import attr

from finitestate.common.enum import AutoName


@unique
class ObservationDataType(AutoName):
    """
    Defines the supported data types for Observation Verifications.
    Should match the types defined here: https://github.com/FiniteStateInc/finite-state-app/blob/master/iotasphere/issue_mgmt/models/__init__.py#L251

    Python treats all floating point numbers as the type float, and stores them internally as a double, so we match that
    treatment here, where we conform the Presto/Athena typing of FLOAT/REAL and DOUBLE as being of type FLOAT for O&T.
    """
    BOOLEAN = auto()
    INTEGER = auto()
    JSON = auto()
    TEXT = auto()
    URI = auto()
    FLOAT = auto()

    @classmethod
    def from_str(cls, string: str) -> 'ObservationDataType':
        if string:
            string = string.strip().upper()

            try:
                return cls[string]
            except KeyError as e:
                # Accept common aliases
                if string == 'BOOL':
                    return cls.BOOLEAN
                if string in ['STR', 'STRING', 'VARCHAR']:
                    return cls.TEXT
                if string == 'INT':
                    return cls.INTEGER
                if string in ['FLOAT', 'REAL', 'DOUBLE']:
                    return cls.FLOAT
                raise e


def conform_data_type(value: Optional[Any], data_type: ObservationDataType) -> Any:
    """
    Conform a value to the expected observation data type.  The intent is to be somewhat flexible in terms of
    type coercion, but not so flexible that obviously incorrectly values are accepted.

    :param value: The value to conform
    :param data_type: The data type to which the value should be conformed
    :return: A value conformed to the appropriate type
    :raises ValueError: On failure to conform the value to the data type.
    """
    if value is None:
        return None

    if data_type == ObservationDataType.BOOLEAN:
        # Accept booleans
        if isinstance(value, bool):
            return value
        # Accept truthy strings
        if isinstance(value, str):
            return strtobool(value)
        # Accept 0s and 1s but no other ints
        if isinstance(value, int):
            if value in [0, 1]:
                return bool(value)

    if data_type == ObservationDataType.INTEGER:
        # Accept integers
        if isinstance(value, int):
            return value
        # Accept integer strings
        if isinstance(value, str):
            return int(value)
        # Accept boolean converted to 0/1
        if isinstance(value, bool):
            return int(value)

    if data_type == ObservationDataType.JSON:
        # Accept dicts or lists
        if isinstance(value, dict) or isinstance(value, list):
            return value
        # Accept strings that can be loaded
        if isinstance(value, str):
            return json.loads(value)

    if data_type == ObservationDataType.TEXT:
        # Accept strings
        if isinstance(value, str):
            return value
        # Accept bytes
        if isinstance(value, bytes):
            return value.decode('utf-8')
        # Accept ints
        if isinstance(value, int):
            return str(value)

    if data_type == ObservationDataType.URI:
        result = urlsplit(str(value))
        if all([result.scheme, result.netloc]):
            return urlunsplit(result)

    if data_type == ObservationDataType.FLOAT:
        if isinstance(value, float):
            return value
        # Convert from acceptable types
        if any([isinstance(value, t) for t in [int, str, Decimal]]):
            return float(value)

    raise ValueError(f'{value} cannot be conformed to {data_type}')


@attr.dataclass
class ObservationField:
    name: str = attr.ib(kw_only=True)
    optional: bool = attr.ib(default=False, kw_only=True)
    data_type: ObservationDataType = attr.ib(
        kw_only=True,
        converter=lambda arg: ObservationDataType.from_str(arg) if isinstance(arg, str) else arg,
        validator=attr.validators.instance_of(ObservationDataType)
    )


@unique
class RequiredFieldMode(AutoName):
    # When operating in NON_NULL_VALUE mode, any field marked as required must have a non-NULL (None) value.
    NON_NULL_VALUE = auto()
    # When operating in FIELD_EXISTS mode, any field marked as required simply has to appear in the verification dict.
    FIELD_EXISTS = auto()


@attr.dataclass
class ObservationVerificationBuilder:
    """
    A class that can conform Python dictionaries to the structure and typing of an Observation Verification Definition.
    """

    fields: List[ObservationField] = attr.ib(factory=list)
    required_field_mode: RequiredFieldMode = attr.ib(kw_only=True, default=RequiredFieldMode.FIELD_EXISTS)

    @classmethod
    def from_graphql_nodes(cls, nodes: Iterable[Dict[str, Any]]) -> 'ObservationVerificationBuilder':
        fields = []

        for node in nodes:
            field = ObservationField(
                name=node['keyName'],
                optional=node['optional'],
                data_type=node['dataType']
            )
            fields.append(field)

        return ObservationVerificationBuilder(fields)

    @classmethod
    def from_graphql_edges(cls, edges: Optional[List[Dict[str, Any]]]) -> 'ObservationVerificationBuilder':
        return cls.from_graphql_nodes([edge['node'] for edge in edges or []])

    def build_verification(self, source: Optional[Dict[str, Any]], include_missing_optional_fields: bool = True) -> Dict[str, Any]:
        verification = {}

        if not source:
            source = {}

        for field in self.fields:
            value = source.get(field.name)
            if not field.optional and value is None:
                if self.required_field_mode == RequiredFieldMode.NON_NULL_VALUE or field.name not in source:
                    raise ValueError(f'{field.name} is required but not found in {source}')
            if field.optional and value is None and not include_missing_optional_fields:
                continue

            try:
                verification[field.name] = conform_data_type(value, field.data_type)
            except Exception as e:
                raise ValueError(f'{field.name} : {value} cannot be conformed to {field.data_type}') from e

        return verification

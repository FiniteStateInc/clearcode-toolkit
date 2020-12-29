"""
    model.py

        Model file representing an Observation message in the Finite State system.

    Typical usage:

        msg = model.ObservationMessage(
                header=model.ObservationMessageHeader(
                    producer='myplugin'),
                payload=[
                    model.ObservationPayload(
                        observation_definition='my_def',
                        firmware_sha256='my_SHA',
                        verifications={'cpes': {'cpe1': 'my_CPE1'}})
                ])
        kafka_producer.send(topic='mytopic', value=msg.serialize_to_bytes())

    ObservationMessage.serialize_to_bytes sample output:

        b'{"version":1,"message_type":"record_set","message_id":"00000000000000000000000000000000",
           "header":{"action":"new_or_update","number_of_records":1,"producer":"myplugin"},
           "payload":[
               {"observation_definition":"my_def","test_definition":"00000000000000000000000000000000",
                "test_version":1,"test_start":"2020-09-03T02:28:32.775607+00:00","test_end":"2020-09-03T02:28:32.775814+00:00",
                "test_trigger_method":"plugin","firmware_sha256":"my_SHA","verifications":{"cpes":{"cpe1":"my_CPE1"}}}
            ]}'
"""

import attr
import datetime
import json
from enum import Enum, unique
from typing import List, Dict
from uuid import UUID, uuid4

from finitestate.common.dateutil import utcnow
from finitestate.common.json_utils import JsonClassSerializable


@unique
class ObservationTriggerMethodType(Enum):
    """Enum representing standard Finite State-defined Observation test trigger methods."""
    ALL_FIRMWARES = 'all_firmwares'
    SINGLE_FIRMWARE = 'single_firmware'
    MANUAL = 'manual'
    PLUGIN = 'plugin'

    def __str__(self):
        return self.value


def _isoformat(dt):
    if isinstance(dt, datetime.datetime):
        return dt.isoformat()
    return dt


@attr.s(auto_attribs=True)
class ObservationPayload(object):
    """Class representing standard Finite State-defined Observation payloads in messages."""
    observation_definition: str = attr.ib(kw_only=True)
    test_definition: UUID = attr.ib(default=UUID('00000000-0000-0000-0000-000000000000'), kw_only=True)
    test_start: str = attr.ib(factory=utcnow, kw_only=True, converter=_isoformat)
    test_end: str = attr.ib(factory=utcnow, kw_only=True, converter=_isoformat)
    test_trigger_method: ObservationTriggerMethodType = attr.ib(default=ObservationTriggerMethodType.PLUGIN, kw_only=True)
    firmware_sha256: str = attr.ib(kw_only=True)
    verifications: Dict = attr.ib(kw_only=True)


@unique
class ObservationMessageType(Enum):
    """Enum representing standard Finite State-defined Observation message types."""
    RECORD_SET = 'record_set'

    def __str__(self):
        return self.value


@unique
class ObservationActionType(Enum):
    """Enum representing standard Finite State-defined Observation actions."""
    NEW = 'new'
    UPDATE = 'update'
    NEW_OR_UPDATE = 'new_or_update'
    DELETE = 'delete'

    def __str__(self):
        return self.value


@attr.s(auto_attribs=True)
class ObservationMessageHeader(object):
    """Class representing standard Finite State-defined Observation message headers."""
    action: ObservationActionType = attr.ib(default=ObservationActionType.NEW_OR_UPDATE, kw_only=True)
    number_of_records: int = attr.ib(default=1, kw_only=True)
    producer: str = attr.ib(kw_only=True)


@attr.s(auto_attribs=True)
class ObservationMessage(object):
    """Class representing standard Finite State-defined Observation messages."""
    version: int = attr.ib(default=1, kw_only=True)
    message_type: ObservationMessageType = attr.ib(default=ObservationMessageType.RECORD_SET, kw_only=True)
    message_id: UUID = attr.ib(factory=uuid4, kw_only=True)
    header: ObservationMessageHeader = attr.ib(kw_only=True)
    payload: List[ObservationPayload] = attr.ib(kw_only=True)

    def serialize_to_bytes(self):
        """Serializes all classes inheriting this to a JSON string."""
        return json.dumps(self, cls=JsonClassSerializable, separators=(',', ':')).encode('utf-8')

import datetime
from abc import ABC
from typing import Any, Dict, Union
from uuid import UUID

from finitestate.common.timer import CodeTimer
from finitestate.firmware.observation.model import ObservationActionType, ObservationMessageHeader, ObservationMessage, ObservationPayload, ObservationTriggerMethodType
from kafka import KafkaProducer

__all__ = [
    'ObservationWriter',
    'KafkaObservationWriter',
]


class ObservationWriter(ABC):
    def write(self, obs_def_id: str, test_id: UUID, firmware_hash: str, verification: Dict[str, Any], test_start: datetime.datetime, test_end: datetime.datetime):
        raise NotImplementedError()

    def close(self):
        pass


class KafkaObservationWriter(ObservationWriter):
    def __init__(self, kafka_producer: KafkaProducer, observation_topic: str, test_trigger_method: ObservationTriggerMethodType, producer_name: str = 'observatory'):
        self.kafka_producer = kafka_producer
        self.observation_topic = observation_topic
        self.test_trigger_method = test_trigger_method
        self.producer_name = producer_name
        self.header = ObservationMessageHeader(
            producer=producer_name,
        )
        self.kafka_futures = []

    def write(self, obs_def_id: str, test_id: Union[str, UUID], firmware_hash: str, verification: Dict[str, Any], test_start: datetime.datetime, test_end: datetime.datetime):
        if test_id is not None and not isinstance(test_id, UUID):
            test_id = UUID(test_id)

        # All observations for a single (test_id, firmware_hash) are sent in a single message by convention,
        # so we take advantage of that and assign the message key from those fields so that messages are
        # deterministically routed to the same partition of the target topic and consumed in the order produced.
        
        key = f"{test_id}:{firmware_hash}".encode()

        msg = ObservationMessage(
            header=self.header,
            payload=[
                ObservationPayload(
                    observation_definition=obs_def_id,
                    test_definition=test_id,
                    test_trigger_method=self.test_trigger_method,
                    test_start=test_start.isoformat(),
                    test_end=test_end.isoformat(),
                    firmware_sha256=firmware_hash,
                    verifications=verification,
                )
            ]
        )

        self.kafka_futures.append(
            self.kafka_producer.send(self.observation_topic, key=key, value=msg.serialize_to_bytes())
        )

    def write_delete(self, obs_def_id: str, test_id: Union[str, UUID], firmware_hash: str):
        # Note: While it would undoubtedly be faster to send multiple payloads per message, we continue to send
        # delete messages 1:1 so that the key of the message can be the same as the new/create message, ensuring
        # that creates and deletes are written to the same Kafka partitions and processed in a time-ordered fashion.
        # If we were to send deletes with a different key, it would be possible to process a delete out of order with
        # its associated create if there were a backlog of messages being reprocessed.

        key = f"{test_id}:{firmware_hash}".encode()

        msg = ObservationMessage(
            header=ObservationMessageHeader(
                producer=self.producer_name,
                action=ObservationActionType.DELETE,
            ),
            payload=[
                ObservationPayload(
                    observation_definition=obs_def_id,
                    test_definition=test_id,
                    test_trigger_method=self.test_trigger_method,
                    firmware_sha256=firmware_hash,
                    verifications=None,
                )
            ]
        )

        self.kafka_futures.append(
            self.kafka_producer.send(self.observation_topic, key=key, value=msg.serialize_to_bytes())
        )

    def close(self):
        if self.kafka_futures:
            with CodeTimer(f'Wait for Kafka broker ACKs for {len(self.kafka_futures)} messages'):
                for future in self.kafka_futures:
                    future.get(60)

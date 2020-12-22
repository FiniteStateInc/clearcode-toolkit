import concurrent.futures
import datetime
import itertools
import json
import logging
import os
from typing import Dict, Iterable, List

import kafka

from finitestate.common.retry_utils import retry
from finitestate.common.timer import CodeTimer
from finitestate.firmware.datasets import Dataset, Density, Format, Granularity
from finitestate.firmware.bundle import FirmwareDataBundler, DEFAULT_MAX_WORKERS, get_file_suffix, list_keys

__all__ = [
    'DynamicFirmwareDataBundler'
]

logger = logging.getLogger(__name__)


class DynamicFirmwareDataBundler(FirmwareDataBundler):
    """
    A FirmwareDataBundler that leverages kafka messages and the status tracker build a bundle containing recent
    FWAN plugin output.  The intended use of this implementation is to build a "delta" bundle to a first pass that
    includes all previously analyzed outputs (since the unpack process doesn't re-analyze previously-seen files).
    """

    def __init__(self, firmware_metadata_bucket: str, *, max_workers: int = DEFAULT_MAX_WORKERS, kafka_bootstrap_server: str, topic: str, earliest_offset_time: datetime.datetime):
        """
        Creates a new DynamicFirmwareDataBundler that will leverage kafka to determine which
        paths should go in the bundle.

        Note that instances of this class aren't re-usable, since they're associated to a specific timestamp.

        :param firmware_metadata_bucket: The bucket from which to read objects
        :param max_workers: The maximum number of parallel threads with which objects will be read from object storage.
        :param earliest_offset_time: The earliest time at which a relevant message may be found on the topic.
        """
        super().__init__(firmware_metadata_bucket=firmware_metadata_bucket, max_workers=max_workers)
        self.kafka_bootstrap_server = kafka_bootstrap_server
        self.topic = topic
        self.earliest_offset_time = earliest_offset_time

    def build_paths(self, firmware_hash: str, datasets: List[Dataset], file_hashes: Iterable[str], delta_to: Dict[str, str] = None) -> List[str]:
        @retry(on='kafka.errors.NoBrokersAvailable')
        def get_kafka_consumer():
            return kafka.KafkaConsumer(bootstrap_servers=self.kafka_bootstrap_server, enable_auto_commit=False)

        with CodeTimer('Connect to kafka'):
            consumer = get_kafka_consumer()

        with CodeTimer(f'Determine partitions and end offsets for {self.topic}'):
            topicparts = [kafka.TopicPartition(self.topic, p) for p in consumer.partitions_for_topic(self.topic) or []]
            end_offsets = consumer.end_offsets(topicparts)

        logger.info(f'End offsets = {end_offsets}')

        # Determine the start offsets using the earliest offset time

        if self.earliest_offset_time is not None:
            timestamp_millis = self.earliest_offset_time.timestamp() * 1000

            with CodeTimer(f'Find offsets on {self.topic} for unpack timestamp'):
                start_offset_times = consumer.offsets_for_times(
                    {topicpart: timestamp_millis for topicpart in topicparts}
                )

            logger.info(f'Start offset times = {start_offset_times}')
        else:
            logger.warning(f'Earliest offset time is not specified - consuming all messages on topic')
            start_offset_times = {}

        consumer = None

        def get_paths_from_topicpart(topicpart: kafka.TopicPartition, datasets: List[Dataset], start_offset: int, stop_offset: int, delta_to: Dict[str, str] = None) -> List[str]:
            consumer = get_kafka_consumer()

            consumer.assign([topicpart])

            if start_offset is not None:
                consumer.seek(topicpart, offset=start_offset)
            else:
                consumer.seek_to_beginning(topicpart)

            dataset_lookup = {d.name: d for d in datasets if d.granularity == Granularity.FILE}

            paths = set([])

            def get_path(record) -> str:
                """
                Parses and creates a path from the record value, which is expected to look like

                {
                  "output_location": "bin_info/exports",
                  "file_id": "<sha256 file hash>",
                  "etag": "object storage checksum of the object at the time it was written"
                }

                :param record: A kafka record
                :return: An object storage path
                """
                try:
                    entity = json.loads(record.value)
                    dataset = dataset_lookup.get(entity['output_location'])
                    if dataset:
                        file_hash = entity['file_id']
                        if file_hash in file_hashes:
                            path = f'{dataset.name}/{file_hash}{get_file_suffix(dataset.format)}'

                            if delta_to is None or entity['etag'] != delta_to.get(path):
                                return path
                except json.JSONDecodeError as e:
                    logger.warning(f'Failed to de-serialize {record}')
                except KeyError:
                    logger.warning(f'Failed to process {record}')

            keep_polling = True

            while keep_polling:
                response = consumer.poll(max_records=1000, timeout_ms=60 * 1000).values()

                if not response:
                    logger.warning(f'Timed out waiting for records on {topicpart} without hitting the stop offset - exiting polling loop')
                    break

                for records in response:
                    for record in records:
                        paths.add(get_path(record))

                        if record.offset >= stop_offset:
                            logger.info(f'Reached stop offset for {topicpart} - exiting polling loop')
                            keep_polling = False

            return sorted(filter(None, paths))

        paths = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

            # Process FILE-level Datasets by looking for recent plugin outputs on Kafka

            for topicpart in topicparts:
                start_offset_time = start_offset_times.get(topicpart)

                futures.append(
                    # stop_offset = end_offset - 1 because the end_offset points to the offset of the upcoming message,
                    # i.e. the offset of the last available message + 1
                    executor.submit(
                        get_paths_from_topicpart,
                        topicpart=topicpart,
                        datasets=datasets,
                        start_offset=start_offset_time.offset if start_offset_time is not None else None,
                        stop_offset=end_offsets[topicpart] - 1,
                        delta_to=delta_to
                    )
                )

            # Process FIRMWARE- and FIRMWARE_FILE-level outputs by directly accessing S3

            for dataset in filter(lambda d: d.name != 'file_tree', datasets):
                file_suffix = get_file_suffix(dataset.format)

                if dataset.granularity == Granularity.FIRMWARE:
                    paths.append(f'{os.path.join(dataset.name, firmware_hash)}{file_suffix}')
                elif dataset.granularity == Granularity.FIRMWARE_FILE:
                    futures.append(
                        executor.submit(
                            list_keys,
                            bucket=self.firmware_metadata_bucket,
                            prefix=f'{dataset.name}/{firmware_hash}/',
                            suffix=file_suffix,
                            delta_to=delta_to
                        )
                    )

        return sorted(set(itertools.chain(paths, *[f.result() for f in concurrent.futures.as_completed(futures)])))

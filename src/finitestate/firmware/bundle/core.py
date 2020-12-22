import collections
import concurrent.futures
import datetime
import itertools
import json
import logging
import os
import random
import shutil
import tarfile
import tempfile
import threading
from abc import ABC, abstractmethod
from contextlib import closing
from typing import BinaryIO, Dict, IO, Iterable, List, Optional, Set, TextIO, Union

import attr
import boto3
import mgzip
from botocore.client import ClientError
from humanize.filesize import naturalsize
from humanize.time import naturaldelta
from more_itertools import chunked
from mypy_boto3_s3 import Client
from mypy_boto3_s3.service_resource import ObjectSummary, S3ServiceResource

from finitestate.common.json_utils import write_jsonl
from finitestate.common.timer import CodeTimer
from finitestate.firmware.bloomfilter import KEY_PREFIX
from finitestate.firmware.bloomfilter.client.core import BloomFilterClient
from finitestate.firmware.datasets import Dataset, Density, Format, Granularity

logger = logging.getLogger(__name__)

# 512 MB - we expect very few files to approach anywhere near this size
# and we're trying _hard_ to stay entirely memory resident for performance
DEFAULT_SPOOLED_FILE_MAX_SIZE = 512 * 1024 * 1024

# Number of threads over which to parallelize bundle path building and fetching
DEFAULT_MAX_WORKERS = 24


FILE_TREE_DATASET = Dataset(name='file_tree', granularity=Granularity.FIRMWARE, density=None, format=Format.JSONL)


@attr.dataclass
class ObjectInfo:
    path: str
    etag: str
    size: int
    last_modified: datetime.datetime

    def __attrs_post_init__(self):
        """
        If last_modified was created with a float, convert it to a datetime.datetime assuming
        it is an epoch seconds value
        """
        if isinstance(self.last_modified, float):
            self.last_modified = datetime.datetime.fromtimestamp(self.last_modified)


@attr.dataclass
class FetchResult:
    info: ObjectInfo
    payload: BinaryIO


@attr.dataclass
class ShardSpec:
    index: int
    count: int


def __serialize_datetime_as_float(obj):
    if isinstance(obj, datetime.datetime):
        return obj.timestamp()
    raise TypeError(f'Type not serializable: {type(obj)}')


def write_object_infos_to_jsonl(infos: List[ObjectInfo], fp: Union[BinaryIO, TextIO]):
    write_jsonl(entities=[attr.asdict(info) for info in infos], fp=fp, default=__serialize_datetime_as_float)


def read_object_infos_from_jsonl(fp: Union[BinaryIO, TextIO]) -> List[ObjectInfo]:
    return [ObjectInfo(**json.loads(line)) for line in fp]


thread_data = threading.local()


def get_thread_local_resource() -> S3ServiceResource:
    """
    Creates or returns a boto3 s3 service that is thread-specific.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    """
    try:
        s3_resource = thread_data.s3_resource
    except AttributeError:
        s3_resource = None

    if not s3_resource:
        thread_data.s3_resource = s3_resource = boto3.resource('s3')

    return s3_resource


def get_thread_local_client() -> Client:
    """
    Creates or returns a boto3 s3 client that is thread-specific.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    """
    try:
        client = thread_data.client
    except AttributeError:
        client = None

    if not client:
        thread_data.client = client = boto3.client('s3')

    return client


def fetch_object(bucket: str, key: str, compare_etag: str = None, max_spool_size: int = DEFAULT_SPOOLED_FILE_MAX_SIZE) -> Optional[FetchResult]:
    """
    Retrieves an object from object storage, loading the content into a spooled temporary file.

    :param bucket: The bucket in which the object lives
    :param key: The object key
    :param compare_etag: If provided, the object will only be fetched if its ETag does NOT match.
    :param max_spool_size: The maximum size of the spooled file to be held in memory
    :returns: An instance of FetchResult which has information about the object and the payload
    """
    args = {'Bucket': bucket, 'Key': key}

    if compare_etag:
        args['IfNoneMatch'] = compare_etag

    obj = get_thread_local_client().get_object(**args)
    etag = obj['ETag'].replace('"', '')

    if compare_etag and etag == compare_etag:
        logger.warning(f'Application of IfNoneMatch may not have worked - skipping object with unmodified ETag')
        return None

    content_length = obj['ContentLength']
    last_modified = obj['LastModified']

    payload: BinaryIO = tempfile.SpooledTemporaryFile(max_size=max_spool_size)

    with closing(obj['Body']) as f:
        shutil.copyfileobj(f, payload)
        buff_len = payload.tell()

    payload.seek(0)

    return FetchResult(ObjectInfo(path=key, etag=etag, size=buff_len, last_modified=last_modified), payload=payload)


def get_file_suffix(format: Format) -> str:
    """
    Returns the suffix with which files of a particular format are persisted in object storage
    :param format: The format
    :return: The suffix for the format
    """
    if format == Format.JSONL:
        return '.jsonl'
    if format == Format.JSON:
        return '.json'
    if format == Format.TARGZ:
        return '.tar.gz'
    return ''


def list_keys(bucket: str, prefix: str, suffix: str, delta_to: Dict[str, str] = None) -> List[str]:
    """
    Lists all the keys belonging to a give key prefix in object storage
    :param bucket: The object storage bucket
    :param prefix: The key prefix
    :param suffix: The key suffix (applied after reading from object storage API)
    :param delta_to: If specified, only keys for objects with modified ETags will be returned
    :return: A list of keys
    """
    def is_included(obj: ObjectSummary):
        return obj.key.endswith(suffix) and (delta_to is None or obj.e_tag.replace('"', '') != delta_to.get(obj.key))

    objects = get_thread_local_resource().Bucket(bucket).objects.filter(Prefix=prefix)
    return [obj.key for obj in objects if is_included(obj)]


def add_to_tarfile(tar: tarfile.TarFile, fetch_result: FetchResult):
    """
    Adds an entity fetched from object storage to a tarfile.  The name (path), size, and mtime of the record in the
    tarfile will be set to match the information retrieved from object storage.

    :param tar: The tarfile that should be appended to
    :param fetch_result: The metadata and payload fetched from object storage
    """
    with closing(fetch_result.payload):
        info = tarfile.TarInfo()
        info.name = fetch_result.info.path
        info.size = fetch_result.info.size
        info.mtime = fetch_result.info.last_modified.timestamp()
        tar.addfile(info, fetch_result.payload)


def extract_file_hashes(file_tree_bytes: BinaryIO) -> Set[str]:
    """
    Returns the set of unique, non-null file hashes from the file tree.

    :param file_tree_bytes: The payload of a file tree read from object storage
    """
    file_hashes = set([])

    for line in file_tree_bytes:
        file_hash = json.loads(line)['file_hash']
        if file_hash:
            file_hashes.add(file_hash)

    file_tree_bytes.seek(0)

    return file_hashes


def get_shard_spec(index: Optional[int], count: Optional[int]) -> Optional[ShardSpec]:
    """
    Returns a validated ShardSpec
    :param index: The index of this shard
    :param count: The total number of shards
    """
    if len([x for x in (index, count) if x is not None]) == 1:
        raise Exception('Incomplete ShardSpec - set both the index and the count')

    if count is None:
        return None

    index = int(index)
    count = int(count)

    if count < 0 or index < 0 or index >= count:
        raise Exception('Invalid ShardSpec - specify a positive count of shards and an index between [0, count)')

    return ShardSpec(index=index, count=count)


def is_dataset_in_shard(dataset: Dataset, shard_spec: ShardSpec) -> bool:
    """
    If sharding is enabled (to build bundles in parallel), FIRMWARE- and FIRMWARE_FILE-level granularity
    Datasets will only appear in the first (0 index) shard because there isn't an effective way to split them.
    Note: The process of building all shards will still utilize the file tree, but it will only be included in one shard.
    """
    return shard_spec is None or shard_spec.index == 0 or dataset.granularity == Granularity.FILE


class FirmwareDataBundler(ABC):
    """
    This class build a *.tar.gz bundle of all object storage content (files) for a firmware, using the firmware's
    file tree to access the relevant data from file-hash-specific datasets (FWAN plugin output locations).
    """
    def __init__(self, firmware_metadata_bucket: str, *, max_workers: int = DEFAULT_MAX_WORKERS):
        self.firmware_metadata_bucket = firmware_metadata_bucket
        self.max_workers = max_workers

    @abstractmethod
    def build_paths(self, firmware_hash: str, datasets: List[Dataset], file_hashes: Iterable[str], delta_to: Dict[str, str] = None) -> List[str]:
        """
        Returns a list of paths (keys) that should be fetched from object storage when building the data bundle. Various
        implementations may rely on different mechanisms/criteria for building the paths.

        :param firmware_hash: The firmware hash that is being bundled
        :param datasets: The list of datasets (FWAN plugin output locations) to include in the bundle
        :param file_hashes: The file hashes to include in the bundle
        :param delta_to: If specified, implementations should make a _best effort_ attempt at only returning modified
        paths - this is especially helpful for FIRMWARE_FILE-level Datasets where a bucket-listing is likely involved.
        The core bundler logic will correctly build a delta bundle even if the implementation returns unmodified paths.
        """
        raise NotImplementedError()

    def bundle(self, firmware_hash: str, datasets: List[Dataset], *, file: Union[str, BinaryIO, IO[bytes]], shard_spec: ShardSpec = None, delta_to: Dict[str, str] = None, overwrite: bool = False) -> List[ObjectInfo]:
        """
        Builds a data bundle (*.tar.gz) for a firmware hash, including content from the
        specified datasets (FWAN plugin output locations)

        :param firmware_hash: The firmware hash to bundle
        :param datasets: The datasets to include in the bundle
        :param file: The output path or file-like-object to which the *.tar.gz output should be written
        :param shard_spec: If provided, only the specified shard of file hashes will appear in the bundle
        :param delta_to: A dictionary of path->etag values, which if supplied will cause the bundle to be built as a
        delta to that set, meaning only new objects or objects with modified etags will appear in the bundle.
        :param overwrite: The output path will not be overwritten, to prevent accidental data loss, unless this is set
        :return: A list of the object included in the bundle
        """
        if not firmware_hash:
            raise ValueError('firmware_hash must be specified')

        if not datasets:
            raise ValueError('datasets must be specified, and non-empty')

        if delta_to is None:
            delta_to = {}

        logger.info(f"Building {'delta' if delta_to else ''} bundle for {firmware_hash}")

        contents: List[ObjectInfo] = []

        with mgzip.open(filename=file, mode='w' if overwrite else 'x') as gz, tarfile.open(fileobj=gz, mode='w', bufsize=tarfile.RECORDSIZE * 4) as tar:
            # Fetch and process the file tree, using that as the basis for all other paths that need to be bundled.

            file_tree_path = f'file_tree/{firmware_hash}.jsonl'

            with CodeTimer('Read firmware file tree from object storage'):
                try:
                    file_tree_result = fetch_object(
                        bucket=self.firmware_metadata_bucket,
                        key=file_tree_path
                    )
                except ClientError as e:
                    raise Exception('Firmware file tree could not be read') from e

            with CodeTimer('Extract file hashes from file tree'):
                try:
                    file_hashes = extract_file_hashes(file_tree_result.payload)
                except json.JSONDecodeError as e:
                    raise Exception('Firmware file tree could not be parsed') from e

            if not file_hashes:
                raise Exception('Firmware file tree is empty')

            file_tree_in_bundle = False

            if is_dataset_in_shard(dataset=FILE_TREE_DATASET, shard_spec=shard_spec) and file_tree_result.info.etag != delta_to.get(file_tree_path):
                with CodeTimer('Add file tree to bundle'):
                    add_to_tarfile(tar, file_tree_result)
                    contents.append(file_tree_result.info)
                    file_tree_in_bundle = True
            else:
                logger.info('File tree is unchanged or not part of this shard and will not be included in the bundle')

            file_tree_size = file_tree_result.info.size

            logger.info(
                'File tree num distinct file hashes = {hash_count}; size = {size}'.format(
                    hash_count=len(file_hashes),
                    size=naturalsize(file_tree_size)
                )
            )

            if shard_spec:
                with CodeTimer(f'Limiting file hashes to shard {shard_spec.index}'):
                    def is_in_shard(file_hash: str) -> bool:
                        return int(file_hash, 16) % shard_spec.count == shard_spec.index
                    file_hashes = [file_hash for file_hash in file_hashes if is_in_shard(file_hash)]
                logger.info(f'Sharded num file hashes = {len(file_hashes)}')

            file_tree_result = None

            # Build paths to be bundled

            with CodeTimer('Build paths for bundle'):
                bundle_datasets = [ds for ds in datasets if is_dataset_in_shard(dataset=ds, shard_spec=shard_spec)]
                paths = self.build_paths(firmware_hash=firmware_hash, datasets=bundle_datasets, file_hashes=file_hashes, delta_to=delta_to) or []
                path_count = len(paths)

            # Validate the paths (check for duplicates)

            with CodeTimer('Validate paths'):
                duplicates = [path for path, count in collections.Counter(paths).items() if count > 1]
                if duplicates:
                    raise Exception(f'Bundle paths contained {len(duplicates)} duplicates: {duplicates}')

            total_path_count = path_count + 1 if file_tree_in_bundle else 0

            logger.info(f'Bundle will include at most {total_path_count} paths from object storage')

            fetch_count = 0
            miss_count = 0
            skip_count = 0
            fetch_bytes = 0

            with CodeTimer('Bundle objects'):
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    fetch_start = datetime.datetime.now()

                    with CodeTimer('Submit object storage path retrieval tasks'):
                        # Randomize path ordering to improve the performance of fetches from object storage,
                        # so that a diversity of object key prefixes is being fetched at any one time.

                        random.shuffle(paths)

                        futures = [
                            executor.submit(
                                fetch_object,
                                bucket=self.firmware_metadata_bucket,
                                key=path,
                                compare_etag=delta_to.get(path),
                            ) for path in paths
                        ]

                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result()

                            if result:
                                add_to_tarfile(tar, result)
                                result.payload = None
                                fetch_count += 1
                                fetch_bytes += result.info.size
                                contents.append(result.info)
                            else:
                                skip_count += 1

                            if fetch_count % 1000 == 0:
                                logger.info(
                                    'Bundled {} objects ({}) in {}'.format(
                                        fetch_count,
                                        naturalsize(fetch_bytes),
                                        naturaldelta(datetime.datetime.now() - fetch_start)
                                    )
                                )
                        except ClientError as e:
                            error_code = e.response.get('Error', {}).get('Code')
                            if 'NoSuchKey' in error_code:
                                miss_count += 1
                            elif '304' == error_code:
                                # The ETag on this object was not modified, so it was not returned
                                skip_count += 1
                            else:
                                raise e

        if skip_count:
            logger.info(f'Skipped {skip_count} unmodified objects')

        if miss_count:
            logger.info(f"Made {miss_count} attempts to access object storage paths that didn't exist")

        logger.info(
            'Bundled {} objects ({})'.format(
                fetch_count + (1 if file_tree_in_bundle else 0),
                naturalsize(fetch_bytes + (file_tree_size if file_tree_in_bundle else 0))
            )
        )

        # Validate fetched paths (check that each path was uniquely processed)

        with CodeTimer('Validate fetched paths'):
            fetched_path_counter = collections.Counter([obj.path for obj in contents])
            duplicates = [path for path, count in fetched_path_counter.items() if count > 1]
            if duplicates:
                raise Exception(f'Bundle paths contained {len(duplicates)} duplicates: {duplicates}')

        with CodeTimer('Finalize output'):
            contents = sorted(contents, key=lambda obj: obj.path)

        return contents


class SimpleFirmwareDataBundler(FirmwareDataBundler):
    """
    A FirmwareDataBundler implementation that uses bloom filters to prune I/O for sparse file-level datasets.
    """

    def __init__(self, firmware_metadata_bucket: str, *, max_workers: int = DEFAULT_MAX_WORKERS, bloom_filter_client: BloomFilterClient = None):
        """
        Creates a new SimpleFirmwareDataBundler that can use a bloom filter client to prune I/O for sparse file-level
        datasets.  If no bloom filter client is provided, the list of paths that will be attempted will be "exhaustive",
        meaning that every possible combination of (dataset, file hash) will be attempted, which will result in a high
        volume of object storage I/O for objects that are likely not present.

        :param firmware_metadata_bucket: The bucket from which to read objects
        :param max_workers: The maximum number of parallel threads with which objects will be read from object storage.
        :param bloom_filter_client: A bloom filter client
        """
        super().__init__(firmware_metadata_bucket=firmware_metadata_bucket, max_workers=max_workers)
        self.bloom_filter_client = bloom_filter_client

    def list_filtered_file_hash_paths(self, dataset: Dataset, file_hashes: List[str]) -> List[str]:
        """
        For a given dataset, returns paths for only those file_hashes that appear in the bloom filters.
        :param dataset: The dataset for which to build paths
        :param file_hashes: The file hashes
        """
        key = f'{KEY_PREFIX}{dataset.name}'
        file_suffix = get_file_suffix(dataset.format)

        return [
            f'{os.path.join(dataset.name, file_hash)}{file_suffix}' for file_hash in
            self.bloom_filter_client.exists(key=key, objects=file_hashes)
        ]

    def build_paths(self, firmware_hash: str, datasets: List[Dataset], file_hashes: Iterable[str], delta_to: Dict[str, str]= None) -> List[str]:
        paths = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

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
                            delta_to=delta_to,
                        )
                    )
                elif dataset.granularity == Granularity.FILE:
                    if self.bloom_filter_client is not None:
                        for chunk in chunked(file_hashes, n=1000):
                            futures.append(
                                executor.submit(
                                    self.list_filtered_file_hash_paths,
                                    dataset=dataset,
                                    file_hashes=chunk,
                                )
                            )
                    else:
                        paths += [
                            f'{os.path.join(dataset.name, file_hash)}{file_suffix}' for file_hash in file_hashes
                        ]
                else:
                    raise Exception(f'Unsupported Dataset Granularity: {dataset.granularity}')

            return list(sorted(itertools.chain(paths, *[f.result() for f in concurrent.futures.as_completed(futures)])))

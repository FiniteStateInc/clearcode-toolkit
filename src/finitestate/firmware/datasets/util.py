import attr
import gzip
import json
import logging
from contextlib import closing
from typing import BinaryIO, IO, List, Union, TextIO

import boto3

from finitestate.common.aws.catalogutils import using_boto3 as get_catalog
from finitestate.common.aws.s3 import get_bucket_and_key_from_uri
from finitestate.common.enum import deep_enum_to_str
from finitestate.common.json_utils import write_jsonl
from finitestate.firmware.datasets import Dataset, Density, Format, Granularity


__all__ = [
    'datasets_from_jsonl',
    'load_latest_datasets',
    'datasets_to_jsonl',
]


logger = logging.getLogger(__name__)


def datasets_from_jsonl(fp: Union[BinaryIO, TextIO, IO[bytes], IO[str]], ignore_errors: bool = False) -> List[Dataset]:
    """
    Reads Datasets from jsonl
    :param fp: A file-like object
    :param ignore_errors: If set to True, errors will be logged but ignored
    :return: A list of Datasets
    """
    datasets = []

    for line in fp:
        record = json.loads(line)
        try:
            dataset = Dataset(
                name=record.get('name') or record.get('dataset_name'),
                granularity=Granularity[record['granularity']],
                density=Density[record['density']] if record.get('density') is not None else None,
                format=Format[record['format']] if record.get('format') is not None else None,
            )

            datasets.append(dataset)
        except Exception as e:
            if ignore_errors:
                logger.error(f'Failed to extract Dataset from {json.dumps(record, default=str)}')
            else:
                raise e

    return datasets


def datasets_to_jsonl(datasets: List[Dataset], fp: Union[BinaryIO, TextIO]):
    """
    Writes a list of Datasets to jsonl
    :param datasets: The list of Datasets
    :param fp: A file-like object
    """
    write_jsonl(entities=[deep_enum_to_str(attr.asdict(d)) for d in datasets], fp=fp, default=str)


def load_latest_datasets(stage: str, region_name: str = None) -> List[Dataset]:
    """
    Loads the latest Dataset definitions from Athena/S3.

    :param stage: The stage to read the datasets from
    :param region_name: The AWS region in which the Athena table and backing S3 bucket exist
    """

    path = get_catalog(region_name=region_name).get_table_path(
        database_name=f'firmware_{stage}', table_name='fwan_dataset_summary'
    )

    bucket, key = get_bucket_and_key_from_uri(path)

    datasets = []

    for obj in boto3.resource('s3', region_name=region_name).Bucket(bucket).objects.filter(Prefix=f'{key}/'):
        with closing(obj.get()['Body']) as f:
            if obj.key.lower().endswith('.gz'):
                f = gzip.open(filename=f, mode='rb')
            datasets += datasets_from_jsonl(f, ignore_errors=True)

    return sorted(datasets, key=lambda d: d.name)

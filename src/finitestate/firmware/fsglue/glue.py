import base64
import hashlib
import json
import logging
import os
import time
from typing import Iterable

from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row, StructType
from pyspark.sql.utils import AnalysisException

from finitestate.firmware.bloomfilter import get_bloom_filter_key
from finitestate.firmware.schemas.schema_file_tree import file_tree_schema
from finitestate.common.aws.s3 import s3_client, get_bucket_and_key_from_uri
from finitestate.common.aws.catalogutils import using_boto3 as get_data_catalog
from finitestate.common.aws.glue import ExecutorGlobals


logger = logging.getLogger(__name__)


def load_dataframe_from_glue_table(database: str, table_name: str, glue_context) -> DataFrame:
    return glue_context.spark_session.table('{}.{}'.format(database, table_name))


def downselect_dataframe(dataframe: DataFrame, list_of_columns_to_select) -> DataFrame:
    return dataframe.select(*list_of_columns_to_select)


def publish_jsonl_to_s3(key, row, target_bucket, max_retries=5, validate_payload_checksum=False):
    """
    Publishes individual rows to S3 as minified JSON. This assumes that the
    entire 'row' element is written as a single JSON object to the target file.
    'data_type' is the plugin-name or otherwise descriptor of the data that is
    to be written. Additionally, 'row' must have a 'firmware_hash' field.
    """

    payload = json.dumps(row.asDict(recursive=True) if isinstance(row, Row) else row, separators=(',', ':'))

    output = {'Bucket': target_bucket, 'Key': key}

    other_kwargs = {}

    if validate_payload_checksum:
        md5 = base64.b64encode(hashlib.md5(payload.encode()).digest()).decode()
        output['ContentMD5'] = md5
        other_kwargs['ContentMD5'] = md5

    retry = 0
    while retry < max_retries:
        try:
            response = ExecutorGlobals.s3_client().put_object(Bucket=target_bucket, Key=key, Body=payload, **other_kwargs)
            output['ETag'] = response['ETag']
            output['Attempts'] = retry + 1
            return output
        except ClientError:
            retry += 1
            time.sleep(2**retry)

    output['Attempts'] = retry + 1
    return output


def publish_custom_cloudwatch_glue_metric(cloudwatch_client, job_name, job_run_ids, metric_name, value, unit=None, namespace=None):
    for job_run_id in job_run_ids:
        response = cloudwatch_client.put_metric_data(
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [{
                        'Name': 'JobName',
                        'Value': job_name
                    }, {
                        'Name': 'JobRunId',
                        'Value': job_run_id
                    }],
                    'Unit': unit or 'None',
                    'Value': value
                },
            ],
            Namespace=namespace or 'Glue',
        )

        if not response or 200 != response.get('ResponseMetadata', {}).get('HTTPStatusCode'):
            raise Exception('Failed to publish metric: {}'.format(response))


def publish_df_as_jsonl(df, get_key_for_row, target_bucket, row_formatter=None):
    # yapf: disable
    try:
        return df.rdd.map(
            lambda row: publish_jsonl_to_s3(get_key_for_row(row), row_formatter(row) if row_formatter else row, target_bucket, validate_payload_checksum=True)
        ).filter(
            lambda output: output.get('ETag')  # was written to S3
        ).count()
    except Exception as e:
        print('Failed to write row as jsonl: {}'.format(e))
        return 0
    # yapf: enable


def read_firmware_file_tree(glue_database: str, fw_sha256: str) -> DataFrame:
    """
    Reads a firmware file tree from the jsonl files backing the file_tree table defined in the Glue Data Catalog.

    :param glue_database: The name of the Glue database from which to read the files, e.g. fimrware_prod.
    :param fw_sha256: The SHA 256 of the firmware to read.
    """
    file_tree_path = get_data_catalog().get_table_path(glue_database, 'file_tree')
    return SparkSession.builder.getOrCreate().read.json(os.path.join(file_tree_path, f'{fw_sha256}.jsonl'), schema=file_tree_schema)


def read_firmware_level_data(glue_database: str, table_name: str, fw_sha256: str, schema: StructType, extension: str = 'jsonl') -> DataFrame:
    """
    Reads a json/jsonl dataset from a single file identified by a firmware sha256, with the path determined by the
    table projecting that data in the Glue Data Catalog.

    Args:
        glue_database: The name of the Glue database from which to read the file, e.g. firmware_prod
        table_name: The name of the table to read
        fw_sha256: The SHA 256 of the firmware to read
        schema: The PySpark schema for the returned data
        extension: The file extension of the file, typically jsonl which is the default.

    Returns: A PySpark DataFrame of the data from object storage, or an empty DataFrame with the appropriate schema
    """
    path = get_data_catalog().get_table_path(glue_database, table_name)
    spark = SparkSession.builder.getOrCreate()
    try:
        return spark.read.json(os.path.join(path, f'{fw_sha256}.{extension}'), schema=schema)
    except AnalysisException as e:
        logger.exception(f'Failed to read firmware {fw_sha256} data from {path} - returning empty DataFrame')
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def read_sbom(glue_database: str, fw_sha256: str) -> DataFrame:
    """
    Reads an SBoM from the json files backing the sbom table defined in the Glue Data Catalog.

    :param glue_database: The name of the Glue database from which to read the files, e.g. fimrware_prod.
    :param fw_sha256: The SHA 256 of the firmware whose SBoM should be read.
    """
    from finitestate.firmware.schemas.schema_sbom import sbom_schema
    sbom_path = get_data_catalog().get_table_path(glue_database, 'sbom')
    return SparkSession.builder.getOrCreate().read.json(os.path.join(sbom_path, f'{fw_sha256}.json'), schema=sbom_schema)


def read_firmware_analytics_from_tree(glue_database: str, table_name: str, file_tree_df: DataFrame, schema: StructType) -> DataFrame:
    """
    Reads a firmware analytic (e.g. crypto_material) from the jsonl files backing the table for that analytic in the Glue Data Catalog.  The
    set of file hashes to read are obtained from the supplied file_tree DataFrame, which is only required to have the `file_hash` column.

    :param glue_database: The name of the Glue database from which to read the files, e.g. fimrware_prod.
    :param table_name: The name of the table to read
    :param file_tree_df: The file_tree DataFrame
    :param schema: The PySpark schema for the returned data.
    """
    path = get_data_catalog().get_table_path(glue_database, table_name)
    bucket, key_prefix = get_bucket_and_key_from_uri(path)

    def read_file(file_hash: str):
        try:
            for line in ExecutorGlobals.s3_client().get_object(Bucket=bucket, Key=os.path.join(key_prefix, file_hash) + '.jsonl')['Body'].iter_lines():
                yield line.decode('utf-8')
        except Exception as e:
            return None

    # yapf: disable
    file_hashes_rdd = file_tree_df.select(
        'file_hash'
    ).dropna().distinct().rdd.map(
        lambda row: row.file_hash
    )

    redis_host = os.environ.get('REDIS_HOST')

    if redis_host:
        redis_port = int(os.environ.get('REDIS_PORT', '6379'))
        bloom_filter_key = get_bloom_filter_key(key_prefix)

        def check_bloom_filters(partition: Iterable[str]):
            from more_itertools import chunked
            from finitestate.firmware.bloomfilter.client.redis import RedisBloomFilterClient

            client = RedisBloomFilterClient(
                redis_client=ExecutorGlobals.redisbloom_client(host=redis_host, port=redis_port)
            )

            for file_hashes in chunked(partition, n=10000):
                yield from client.exists(key=bloom_filter_key, objects=file_hashes)

        if ExecutorGlobals.redisbloom_client(host=redis_host, port=redis_port).exists(bloom_filter_key):
            logger.info(f'Filtering {glue_database}.{table_name} file hashes according to bloom filter membership in {bloom_filter_key}')
            file_hashes_rdd = file_hashes_rdd.mapPartitions(
                check_bloom_filters
            )
    else:
        logger.warning(f'Performing exhaustive search for files in {key_prefix}; check plugin configuration to enable use of bloom filters')

    return SparkSession.builder.getOrCreate().read.json(
        file_hashes_rdd.flatMap(read_file).filter(lambda x: x), schema=schema
    )

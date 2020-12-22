import concurrent.futures
import datetime
import logging
import os
import random
import re
import string
import time
from codecs import getreader
from contextlib import closing
from functools import lru_cache
from typing import Any, Dict, List, Generator, Optional, Union

from finitestate.common.aws.s3 import get_bucket_and_key_from_uri
from finitestate.common.retry_utils import retry

logger = logging.getLogger(__name__)


class BucketingConfig(object):
    def __init__(self, columns: Union[str, List[str]], count: int):
        if isinstance(columns, str):
            self.columns = [columns]
        else:
            self.columns = columns
        self.count = count


def quote_strings(x):
    if isinstance(x, str):
        return "'{x}'".format(x=x)
    return x


def build_ctas(target_database: str,
               target_table: str,
               target_path: str,
               format: str = 'parquet',
               compression: str = 'SNAPPY',
               bucketing_config: BucketingConfig = None,
               source_database: str = None,
               source_table: str = None,
               columns: Union[str, List[str]] = None,
               query: str = None):

    storage_options = {
        'format': quote_strings(format),
        'external_location': quote_strings(target_path)
    }

    if (format and (format.lower() == 'parquet' or format.lower() == 'orc')) and compression is not None:
        storage_options['{format}_compression'.format(format=format)] = quote_strings(compression)

    if bucketing_config:
        storage_options['bucketed_by'] = 'ARRAY[{}]'.format(','.join([quote_strings(c) for c in bucketing_config.columns]))
        storage_options['bucket_count'] = bucketing_config.count

    storage_stanza = "WITH ({})".format(', '.join(['{k}={v}'.format(k=k, v=v) for k, v in storage_options.items()]))

    if not query:
        if not columns:
            columns = ['*']
        if isinstance(columns, str):
            columns = [columns]

        query = 'SELECT {columns} FROM {source_database}.{source_table}'.format(
            source_database=source_database,
            source_table=source_table,
            columns=', '.join(columns)
        )

    template = 'CREATE TABLE {target_database}.{target_table} {storage_stanza} ' \
               'AS {query}'

    return template.format(target_database=target_database,
                           target_table=target_table,
                           storage_stanza=storage_stanza,
                           query=query)


__athena_client = None
__glue_client = None
__s3_resource = None


def get_athena_client():
    global __athena_client
    if not __athena_client:
        import boto3
        __athena_client = boto3.client('athena', endpoint_url=os.environ.get('ATHENA_ENDPOINT_URL'))
    return __athena_client


def get_glue_client():
    global __glue_client
    if not __glue_client:
        import boto3
        __glue_client = boto3.client('glue', endpoint_url=os.environ.get('GLUE_ENDPOINT_URL'))
    return __glue_client


def get_s3_resource():
    global __s3_resource
    if not __s3_resource:
        import boto3
        __s3_resource = boto3.resource('s3', endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    return __s3_resource


def get_s3_client():
    return get_s3_resource().meta.client


def submit_query(database_name: str, query_string: str, output_location: str) -> str:
    """
    Asynchronously submits a SQL statement to Athena.
    :param database_name: The Glue database name
    :param query_string: The SQL statement to execute
    :param output_location: The S3 location where Athena should store its results
    :return: The Athena query execution ID
    """

    query = get_athena_client().start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": output_location},
    )

    query_execution_id = query["QueryExecutionId"]

    logger.info(f"Submitted Athena query {query_execution_id} : {query_string}")

    return query_execution_id


def wait_for_query(query_execution_id: str, sleep_sec: int = None):
    """
    Waits (blocks) for an asynchronous Athena query to complete.
    :param query_execution_id: The Athena query ID to wait for
    :param sleep_sec: The number of seconds to sleep between calls to the Athena API to check on the query
    :raises Exception: on query entering FAILED or CANCELLED status
    """
    while True:
        check_response = get_athena_client().get_query_execution(
            QueryExecutionId=query_execution_id
        )
        query_state = check_response["QueryExecution"]["Status"]["State"]

        if query_state == "QUEUED" or query_state == "RUNNING":
            time.sleep(sleep_sec or 3)
        elif query_state == "SUCCEEDED":
            break
        elif query_state == "FAILED" or query_state == "CANCELLED":
            raise Exception("Athena query failed: {}".format(query_execution_id))


@lru_cache(maxsize=1000)
def __parser_for_athena_type(athena_type) -> type:
    if athena_type in ['char', 'varchar', 'string']:
        return str
    if athena_type in ['tinyint', 'smallint', 'int', 'integer', 'bigint']:
        return int
    if athena_type == 'boolean':
        return bool
    if athena_type in ['double', 'float', 'real']:
        return float

    raise ValueError(f'Unsupported Athena type: {athena_type}')


def __as_dict(row_data, columns):
    def as_python_type(cell, column) -> Any:
        value = cell.get('VarCharValue')
        if value is not None:
            return __parser_for_athena_type(column['Type'].lower())(value)
        return None

    return {column['Name']: as_python_type(cell, column) for cell, column in zip(row_data, columns)}


def __is_header(row_data, columns):
    return all([cell.get('VarCharValue') == column['Name'] for cell, column in zip(row_data, columns)])


END_OF_VALUE = re.compile(r'"(,|$)')


def split_csv_line(line: str) -> List[Optional[str]]:
    """
    No, the author of this code was not unaware of csv.Reader.  Unfortunately, csv.Reader turns None into ''
    and it may be important for clients of this code to differentiate between missing (None) and blank ('') values
    in the result set being parsed, so we do it ourselves.
    """
    if not line:
        return [None]

    original = line
    line = line.rstrip()
    output = []
    tail = []

    while line[-1] == ',':
        line = line[:-1]
        tail.append(None)

    while line:
        ch = line[0]

        if ch == ',':
            output.append(None)
            line = line[1:]
        elif ch == '"':
            end = END_OF_VALUE.search(line, pos=1)
            if not end:
                raise ValueError(f'Failed to find the end of quoted field while splitting {original}')
            output.append(line[1:end.start()])
            line = line[end.end():]
        else:
            raise ValueError(f'Unexpected character {ch} encountered while splitting {original}')

    return output + tail


def stream_results(query_execution_id: str):
    """
    Reads an Athena result set by directly accessing the CSV file on S3.
    """
    def get_columns(query_execution_id: str):
        return get_athena_client().get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)['ResultSet']['ResultSetMetadata']['ColumnInfo']

    def get_location(query_execution_id: str):
        response = get_athena_client().get_query_execution(QueryExecutionId=query_execution_id)
        return response['QueryExecution']['ResultConfiguration']['OutputLocation']

    with concurrent.futures.ThreadPoolExecutor() as pool:
        get_columns_future = pool.submit(get_columns, query_execution_id)
        get_location_future = pool.submit(get_location, query_execution_id)

    bucket, key = get_bucket_and_key_from_uri(get_location_future.result())

    python_type_mapper = {
        column['Name']: __parser_for_athena_type(column['Type'].lower()) for column in get_columns_future.result()
    }

    logger.debug(f'Reading Athena query results from s3://{bucket}/{key}')

    with closing(getreader('utf-8')(get_s3_client().get_object(Bucket=bucket, Key=key)['Body'])) as lines:
        column_names = split_csv_line(next(lines))

        for line in lines:
            yield {
                k: python_type_mapper[k](v) if v is not None else None for k, v in zip(column_names, split_csv_line(line))
            }


def stream_results_from_api(query_execution_id: str, page_size: int = None) -> Generator[Dict[str, Any], None, None]:
    """
    Reads an Athena result set by paging through it with calls to get_query_results.  The performance of this
    approach is notably slower than direct S3 access for large result sets, because AWS allows a maximum page size
    of 1,000 rows.
    """
    paginator = get_athena_client().get_paginator('get_query_results')
    pages = paginator.paginate(QueryExecutionId=query_execution_id, PaginationConfig={'PageSize': page_size or 1000})

    for page in pages:
        columns = page['ResultSet']['ResultSetMetadata']['ColumnInfo']
        rows = page['ResultSet']['Rows']

        start = 1 if __is_header(rows[0]['Data'], columns) else 0

        for row in rows[start:]:
            yield __as_dict(row['Data'], columns)


def get_table_base_path(database_name: str, table_name: str):
    response = get_glue_client().get_table(
        DatabaseName=database_name,
        Name=table_name
    )

    return response["Table"]["Parameters"]["fs_base_path"]


def ctas_rebuild_table(database_name: str, table_name: str, query: str, bucketing_config: BucketingConfig = None, format: str = 'parquet'):
    # Use a CTAS to create a temporary table with the latest content for the real table

    temp_target_table = "temp_{table}_ctas_{guid}".format(
        table=table_name,
        guid=''.join(random.sample(string.ascii_lowercase, 6))
    )

    target_path = os.path.join(
        get_table_base_path(database_name, table_name),
        "date={date}".format(date=datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S'))
    )

    target_bucket, _ = get_bucket_and_key_from_uri(target_path)
    athena_results = f"s3://{target_bucket}/_athena_results"

    # Create the temp table

    ctas = build_ctas(
        target_database=database_name,
        target_table=temp_target_table,
        target_path=target_path,
        bucketing_config=bucketing_config,
        query=query,
        format=format,
    )

    # Drop the temp table (leaving the data behind)

    drop = "DROP TABLE IF EXISTS {database}.{table}".format(
        database=database_name,
        table=temp_target_table
    )

    # Update the storage location of the main table to use the newly generated data

    update = "ALTER TABLE {database}.{table} SET LOCATION '{location}'".format(
        database=database_name,
        table=table_name,
        location=target_path
    )

    # If we retry after a transient Athena failure, we may need to purge existing partial output.

    def s3_rmr(target_path: str):
        bucket, key = get_bucket_and_key_from_uri(target_path)

        if not bucket or not key:
            raise ValueError("Unsupported target_path value: {}".format(target_path))

        for o in get_s3_resource().Bucket(bucket).objects.filter(Prefix=key):
            o.delete()

    # Athena Queries will sometimes fail with "Query exhausted resources at this scale factor" errors,
    # and the recommendation is to retry with back-off.

    @retry(max_retries=5)
    def process(statements):
        # In the case of a retry, we may need to purge existing partial output.

        s3_rmr(target_path)

        for statement in statements:
            wait_for_query(submit_query(database_name=database_name, query_string=statement, output_location=athena_results))

    process([ctas, drop, update])

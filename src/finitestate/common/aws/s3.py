import datetime
import json
import time

try:
    from urllib.parse import urlparse
except:
    from urlparse import urlparse

import boto3
from botocore.exceptions import ClientError

try:
    from finitestate.common.schema import assert_data_matches_schema
except Exception as e:
    pass


try:
    import finitestate.common.dateutil as fsdt
except Exception as e:
    print("Unable to load module finitestate.common.dateutil: {}".format(e))

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


def put_to_s3_with_retry(bucket, key, body, max_retries=3, content_md5=None, schema=None):
    if schema:
        try:
            assert_data_matches_schema(body, schema)
        except NameError as e:
            print("Unable to load module finitestate.common.schema: {}".format(e))
            raise e

    other_kwargs = {}

    if content_md5:
        other_kwargs['ContentMD5'] = content_md5

    retry = 0
    while retry < max_retries:
        try:
            return s3_resource.Object(bucket, key).put(Body=body, **other_kwargs)
        except ClientError as err:
            if "Rate Exceeded" in err.args[0]:
                retry += 1
                sleep_time = 2**retry
                time.sleep(sleep_time)
            else:
                raise


def put_compliant_object(bucket, key, obj, schema):
    try:
        assert_data_matches_schema(obj, schema)
    except NameError as e:
        print("Unable to load module finitestate.common.schema: {}".format(e))
        raise e

    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj))


def get_bucket_and_key_from_uri(uri):
    parsed_uri = urlparse(uri)
    return parsed_uri.netloc, parsed_uri.path.lstrip('/')


def slurp_object(bucket, key):
    return s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()


def slurp_object_from_uri(uri):
    bucket, key = get_bucket_and_key_from_uri(uri)
    return slurp_object(bucket, key)


def find_aged_objects(base_uri, older_than, excluded_key_prefixes=None):
    """
    A generator function that returns S3 objects under a base location that are older than a specified time.
    :param base_uri: A fully-qualified S3 uri
    :param excluded_key_prefixes: (Optional) A list of excluded S3 object key prefixes
    :param older_than: A datetime.datetime in UTC for absolute comparison or datetime.timedelta for comparison relative to UTC now.
    :return: A generator of boto3 ObjectSummary values.

    To correctly supply an absolute datetime.datetime value in UTC for the older_than argument, make sure that you create
    the datetime in such a manner that it has a defined, unambiguous timezone, e.g. datetime.datetime.now(datetime.timezone.utc)
    instead of datetime.datetime.utcnow()

    See https://docs.python.org/3/library/datetime.html#datetime.datetime.utcnow
    """

    bucket, key = get_bucket_and_key_from_uri(base_uri)

    if not key:
        raise ValueError('Cannot determine key prefix to delete from {}'.format(bucket))
    elif not key.endswith('/'):
        key = key + '/'

    if isinstance(older_than, datetime.datetime):
        def skip(last_modified_utc):
            return last_modified_utc >= older_than
    elif isinstance(older_than, datetime.timedelta):
        now = fsdt.utcnow()

        def skip(last_modified_utc):
            return (now - last_modified_utc) >= older_than
    else:
        raise ValueError('older_than must be a datetime.datetime or datetime.timedelta')

    if isinstance(excluded_key_prefixes, str):
        excluded_key_prefixes = [excluded_key_prefixes]

    for o in s3_resource.Bucket(bucket).objects.filter(Prefix=key):
        if excluded_key_prefixes and any(o.key.startswith(key) for key in excluded_key_prefixes):
            continue

        if skip(fsdt.as_utc(o.last_modified)):
            continue

        yield o

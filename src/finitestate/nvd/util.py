from botocore.exceptions import ClientError
import boto3
import gzip
import json
import logging
import time
from urllib.request import urlopen

logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')


class MessageQueuer:
    '''Conveniently handles sending message batches to SQS.'''

    def __init__(self, queue_url, batch_size=10):
        '''Initialize internal variables'''
        self.batch_size = batch_size
        self.queue_url = queue_url
        self.client = boto3.client('sqs')
        self.queued = []
        self.failed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    def add_string(self, msg):
        '''Enqueue a message.  If there are enough messages to fill a batch, send them.'''
        self.queued.append(msg)
        if len(self.queued) >= self.batch_size:
            self.flush()

    def _send_batch(self, batch):
        '''Send a single batch of messages to SQS, storing failures.'''
        if not batch:
            return

        response = self.client.send_message_batch(
            QueueUrl=self.queue_url,
            Entries=[
                {'Id': f'{hash(msg)}', 'MessageBody': msg}
                for msg in batch if msg
            ]
        )
        failed = response.get('Failed', [])
        self.failed.extend(failed)

    def flush(self):
        '''Send all of the remaining messages to SQS, one batch at a time.'''
        while len(self.queued) > 0:
            batch = self.queued[:self.batch_size]
            self.queued = self.queued[self.batch_size:]
            self._send_batch(batch)


def fetch_feed_sha(feed_url):
    '''Get the feed SHA from the NVD's hosted metadata.'''
    logger.debug('Fetching SHA for {} from NVD'.format(feed_url))
    meta_url = feed_url.replace('.json.gz', '.meta')
    with urlopen(meta_url) as f:
        lines = f.read().decode('utf-8').split('\n')
    fields = dict(line.split(':', 1) for line in lines if line)
    return fields.get('sha256').strip()


def load_cached_sha(feed_url, cache_bucket):
    '''Fetch a feed SHA from our cache in S3.'''
    s3 = boto3.client('s3')
    logger.debug('Loading our cached feed SHA for {}'.format(feed_url))
    key = f'third_party/nvd/feed_shas/{feed_url}.txt'
    try:
        logger.debug(f'getting s3://{cache_bucket}/{key}')
        s3_contents = s3.get_object(Bucket=cache_bucket, Key=key)['Body'].read()
        sha = s3_contents.decode('utf-8')
    except Exception as e:
        logger.warn(f'Couldn\'t get cached feed SHA at s3://{cache_bucket}/{key}')
        sha = None

    return sha


def save_cached_sha(feed_url, sha, cache_bucket):
    '''Update the SHA associated with a feed_url in our S3 cache.'''
    s3 = boto3.client('s3')
    logger.debug('Caching the new SHA for {}'.format(feed_url))
    key = f'third_party/nvd/feed_shas/{feed_url}.txt'
    try:
        logger.debug(f'putting s3://{cache_bucket}/{key}')
        s3.put_object(Bucket=cache_bucket, Key=key, Body=sha)
    except Exception as e:
        logger.exception('Couldn\'t cache SHA for {}'.format(feed_url))
        raise e


def get_feed_data(url):
    '''Fetch, decompress, and parse gzipped JSON data from the given URL.'''
    try:
        logger.debug('Fetching {}'.format(url))
        with urlopen(url) as f:
            compressed = f.read()
    except Exception as e:
        logger.exception('Error while fetching {}'.format(url))
        raise e

    try:
        logger.debug('Decompressing {}'.format(url))
        decompressed = gzip.decompress(compressed)
    except Exception as e:
        logger.exception('Error while decompressing data from {}'.format(url))
        raise e

    try:
        logger.debug('Parsing {}'.format(url))
        parsed = json.loads(decompressed)
    except Exception as e:
        logger.exception('Error while parsing data from {}'.format(url))
        raise e

    return parsed


def feed_has_changed(url, cache_bucket):
    '''Detect whether the NVD feed at the given url has changed since the last time we fetched it.'''
    current_sha = fetch_feed_sha(url)
    cached_sha = load_cached_sha(url, cache_bucket)
    logger.debug(f'old SHA: [{cached_sha}]')
    logger.debug(f'new SHA: [{current_sha}]')
    if current_sha != cached_sha:
        return current_sha


def fetch_from_s3(bucket, key, max_retries=3):
    '''
    Helper function to fetch raw data from an S3 key
    '''
    retry = 0
    while retry < max_retries:
        try:
            logger.debug("Attempting to fetch s3://{}/{}".format(bucket, key))
            # get data
            response = s3.get_object(
                Bucket=bucket,
                Key=key
            )
            body = response['Body']
            data = body.read()
            return data
        except ClientError as e:
            if "Rate Exceeded" in e.args[0]:
                retry += 1
                sleep_time = 2 ** retry
                logger.warning(f'Sleeping for {sleep_time} due to s3 rate limit')
                time.sleep(sleep_time)
            else:
                raise e


def fetch_jsonl_from_s3(bucket, key):
    '''
    Helper function to return a list of JSON as parsed out of an S3 object
    '''
    try:
        data = fetch_from_s3(bucket, key).decode('utf-8')
        return [json.loads(line) for line in data.split('\n') if line]
    except ValueError as e:
        logger.exception("ERROR: Found data for s3://{}/{} but couldn't decode it as JSON".format(bucket, key))
        raise e
    except Exception as e:
        raise e


def fetch_json_from_s3(bucket, key):
    '''
    Helper function to return a single JSON object as parsed out of an S3 object
    '''
    try:
        data = fetch_from_s3(bucket, key).decode('utf-8')
        parsed = json.loads(data)
        return parsed
    except ValueError as e:
        logger.exception("ERROR: Found data for s3://{}/{} but couldn't decode it as JSON".format(bucket, key))
        raise e
    except Exception as e:
        raise e


def save_to_s3(bucket, key, content, max_retries=3):
    logger.debug(f'trying to save {len(content)} bytes to s3://{bucket}/{key}')
    retry = 0
    while retry < max_retries:
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=content)
            break
        except ClientError as e:
            if 'Rate Exceeded' in e.args[0]:
                retry += 1
                sleep_time = 2**retry
                logger.warning(f'Sleeping for {sleep_time} due to s3 rate limit')
                time.sleep(sleep_time)
        except Exception as e:
            logger.exception('Save failed')
            raise e
    if retry >= max_retries:
        logger.exception('Used up all retries')


def save_json_to_s3(bucket, key, obj):
    logger.debug(f'saving s3://{bucket}/{key}')
    body = json.dumps(obj, separators=(',', ':'))
    save_to_s3(bucket, key, body)


def save_jsonl_to_s3(bucket, key, seq):
    logger.debug(f'saving s3://{bucket}/{key}')
    body = '\n'.join(json.dumps(obj, separators=(',', ':')) for obj in seq)
    save_to_s3(bucket, key, body)

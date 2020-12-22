import base64
import datetime
import hashlib
import sys
import uuid
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import ClientError

from finitestate.common.aws.s3 import slurp_object, s3_client, s3_resource, put_to_s3_with_retry


ENTITY_IDENTIFIER = 'entity_identifier'
SNAPSHOT_IDENTIFIER = 'snapshot_identifier'
SNAPSHOT_VERSION = 'snapshot_version'
S3_OBJECT_SIZE = 's3_object_size'
TIMESTAMP = 'timestamp'
CONTENT_MD5 = 'content_md5'
ETAG = 's3_object_etag'


class SnapshotManager(object):
    def __init__(self, s3_bucket_name, s3_key_prefix, ddb_table_name=None, ddb_table_region=None, sns_topic_arn=None, ddb_table=None, schema=None, validate_s3_payload_checksums=False, s3_latest_bucket_name=None, s3_latest_key_prefix=None):
        if s3_bucket_name is None:
            raise Exception("s3_bucket_name is required")
        if s3_key_prefix is None:
            raise Exception("s3_key_prefix is required")

        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

        self.validate_s3_payload_checksums = validate_s3_payload_checksums

        self.s3_latest_bucket_name = s3_latest_bucket_name
        self.s3_latest_key_prefix = s3_latest_key_prefix

        self.schema = schema

        if ddb_table:
            self.ddb_table = ddb_table
        else:
            self.ddb_table = boto3.resource('dynamodb', region_name=ddb_table_region).Table(ddb_table_name)

        if sns_topic_arn is not None:
            self.sns_topic_arn = sns_topic_arn
            self.sns_client = boto3.client('sns')

    def create(self, entity_identifier, data):
        metadata = {
            ENTITY_IDENTIFIER: entity_identifier,
            SNAPSHOT_VERSION: 1,
        }
        return self._write(metadata, data)

    def read(self, entity_identifier, ddb_consistent_read=True):
        query_result = self.ddb_table.query(
            Limit=1,
            ConsistentRead=ddb_consistent_read,
            ScanIndexForward=False,
            KeyConditionExpression=Key(ENTITY_IDENTIFIER).eq(entity_identifier)
        )

        if query_result['Count'] == 0:
            return None

        metadata = query_result['Items'][0]

        try:
            data = slurp_object(self.s3_bucket_name, self.s3_key_prefix + metadata[SNAPSHOT_IDENTIFIER])
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if 'NoSuchKey' in error_code or 'AccessDenied' in error_code:
                data = None
            else:
                raise e

        return Snapshot(self, metadata, data)

    def _update(self, snapshot, data):
        metadata = snapshot.metadata.copy()
        metadata[SNAPSHOT_VERSION] += 1
        return self._write(metadata, data)

    def _write(self, metadata, data):
        snapshot_identifier = str(uuid.uuid4())
        metadata[SNAPSHOT_IDENTIFIER] = snapshot_identifier
        metadata[S3_OBJECT_SIZE] = sys.getsizeof(data)
        metadata[TIMESTAMP] = Decimal(str(datetime.datetime.utcnow().timestamp()))

        # Optionally compute the MD5 digest of the payload for verification by S3.

        if self.validate_s3_payload_checksums:
            md5 = base64.b64encode(hashlib.md5(data.encode()).digest()).decode()
            metadata[CONTENT_MD5] = md5
        else:
            md5 = None

        response = put_to_s3_with_retry(
            bucket=self.s3_bucket_name,
            key=self.s3_key_prefix + snapshot_identifier,
            body=data,
            schema=self.schema,
            content_md5=md5
        )

        # Enrich the DynamoDB metadata with the ETag of the object on S3.

        metadata[ETAG] = response.get('ETag')

        self.ddb_table.put_item(
            Item=metadata,
            ConditionExpression=Attr(SNAPSHOT_VERSION).not_exists()
        )

        # Optionally write the new version of the entity to the separate "latest" path.

        if self.s3_latest_bucket_name or self.s3_latest_key_prefix:
            s3_client.copy(
                CopySource={
                    'Bucket': self.s3_bucket_name,
                    'Key': self.s3_key_prefix + snapshot_identifier,
                },
                Bucket=self.s3_latest_bucket_name,
                Key=self.s3_latest_key_prefix + '{}.0'.format(base64.urlsafe_b64encode(metadata[ENTITY_IDENTIFIER].encode()).decode())
            )

        self.sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Message=metadata[ENTITY_IDENTIFIER]
        )

        return Snapshot(self, metadata, data)


class Snapshot(object):
    def __init__(self, manager, metadata, data):
        self.manager = manager
        self.metadata = metadata
        self.data = data

    def entity_identifier(self):
        return self.metadata[ENTITY_IDENTIFIER]

    def snapshot_identifier(self):
        return self.metadata[SNAPSHOT_IDENTIFIER]

    def snapshot_version(self):
        return self.metadata[SNAPSHOT_VERSION]

    def update(self, data):
        return self.manager._update(self, data)

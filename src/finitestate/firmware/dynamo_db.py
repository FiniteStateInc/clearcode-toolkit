import json
import logging
import time

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def put_dynamo_item_with_retry(table, item, condition=None, max_retries=3):
    """
    Writes an item to a DynamoDB table with retry on limit exceeded errors.
    :param boto3.resources.factory.dynamodb.ServiceResource dynamodb: The boto3 dynamodb service resource
    :param str table: The DynamoDB table name
    :param dict item: The item to write
    :param max_retries:
    :return:
    """
    retry = 0
    while retry <= max_retries:
        try:
            if condition:
                table.put_item(Item=item, ConditionExpression=condition)
            else:
                table.put_item(Item=item)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code != 'ConditionalCheckFailedException':
                if 'Exceeded' not in error_code:
                    break
                retry += 1
                sleep_time = 2**retry
                time.sleep(sleep_time)
            else:
                logger.exception(f'Request failed for record {json.dumps(item, default=str)} and condition {condition}')
                break
        else:
            break

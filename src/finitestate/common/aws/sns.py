import boto3
import json
from finitestate.common.schema import assert_data_matches_schema

def publish_compliant_message(topic_arn, region_name, message, schema):
    assert_data_matches_schema(message, schema)
    boto3.client("sns", region_name=region_name).publish(TopicArn = topic_arn, Message = json.dumps(message))

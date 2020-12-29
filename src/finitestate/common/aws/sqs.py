import boto3
import json
from finitestate.common.schema import assert_data_matches_schema

def receive_compliant_message(sqs_record, schema):
    return assert_data_matches_schema(json.loads(sqs_record["body"]), schema)

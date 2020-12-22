import boto3
import json
from finitestate.common.aws.s3 import put_to_s3_with_retry


def load_approximate_quantile_document_from_s3(bucket: str, quantile_type: str) -> dict:
    s3_resource = boto3.resource('s3')
    # _not_ jsonl
    document_key = 'rollups/firmware/quantiles/{}.json'.format(quantile_type)
    document_object = s3_resource.Object(bucket, document_key)
    return {int(k): v for k, v in json.loads(document_object.get()['Body'].read().decode('utf-8')).items()}


# TODO: Figure out how to get this guy into a library
def write_approximated_risk_score(bucket: str, firmware_hash: str, score_type: str, score: float):
    destination_key = 'rollups/firmware/interval_stats/exploded/{}/{}.json'.format(firmware_hash, score_type)
    approximate_risk_document = json.dumps({'firmware_hash': firmware_hash, score_type: score})
    put_to_s3_with_retry(bucket, destination_key, approximate_risk_document)
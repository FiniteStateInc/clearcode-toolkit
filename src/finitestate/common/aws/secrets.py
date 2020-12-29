# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import base64
import json
import logging
from botocore.exceptions import ClientError

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_secret(secret_name: str, region_name: str = None, secret_key: str = None):
    returnSecret = None

    logger.info("Querying Secrets Manager for {}".format(secret_name))

    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f'Error while retrieving secret: {e}')
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Secrets Manager: Can't decrypt the protected secret text using the provided KMS key.")
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("Secrets Manager: An error occurred on the server side.")
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("Secrets Manager: You provided an invalid value for a parameter: ({secret_name}, {region_name})")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error(
                "Secrets Manager: You provided a parameter value that is not valid for the current state of the resource: ({secret_name}, {region_name})"
            )
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(f"Secrets Manager: We can't find the resource that you asked for: ({secret_name}, {region_name})")
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            try:
                secretDict = json.loads(secret)
            except Exception as e:
                logger.error("Unable to parse secret")
                raise e

            if secret_name in secretDict:
                returnSecret = secretDict[secret_name]
            else:
                returnSecret = secretDict

        elif 'SecretBinary' in get_secret_value_response:
            try:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            except Exception as e:
                logger.error("Unable to decode binary secret")
                raise e
            returnSecret = decoded_binary_secret
        else:
            logger.error("Unexpected Secrets Manager Response (no secrets found in the response.")
            raise KeyError

        # If we really only want part of the secret, and we have it, then return just that part
        # If that part isn't in the secret don't return anything.
        if secret_key is not None and secret_key in returnSecret:
            return returnSecret[secret_key]
        elif secret_key is None:
            return returnSecret
        else:
            logger.warn(f"Unable to find {secret_key} in {secret_name}")
            return None

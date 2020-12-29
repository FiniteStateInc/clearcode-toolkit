from botocore.exceptions import ClientError
import time


def put_dynamo_item_with_retry(table, item, max_retries=3):
    """
    Writes an item to a DynamoDB table with retry on limit exceeded errors.

    :param table: The DynamoDB table object
    :param dict item: The item to write
    :param max_retries: The maximum number of retries - defaults to 3
    """
    retry = 0
    while retry <= max_retries:
        try:
            table.put_item(Item=item)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if "Exceeded" not in error_code:
                raise
            retry += 1
            sleep_time = 2 ** retry
            time.sleep(sleep_time)
        else:
            break
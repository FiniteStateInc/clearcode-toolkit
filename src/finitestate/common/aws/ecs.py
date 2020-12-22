import datetime
import json
import logging
import os
import time
from typing import Any, Dict

import boto3

__all__ = [
    'check_is_instance_draining',
    'read_container_metadata',
]


logger = logging.getLogger(__name__)

__ecs_client = None


def __get_ecs_client():
    global __ecs_client
    if __ecs_client is None:
        __ecs_client = boto3.client('ecs')
    return __ecs_client


def read_container_metadata(wait_for_ready: bool = True, timeout_sec: int = None) -> Dict[str, Any]:
    """
    Reads ECS container metadata.
    See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/container-metadata.html for more information.
    If the ECS_CONTAINER_METADATA_FILE environment variable isn't set, or the file it points to doesn't exist
    or can't be read, an empty dictionary will immediately be returned.

    Args:
        wait_for_ready: If True (default), will poll for the file to indicate that it is fully populated/ready.
        timeout_sec: The maximum time in seconds to wait for the metadata file to be ready.

    Returns:
        Dict[str, Any]: A dictionary with the fields specified by AWS documentation.
    """
    metadata_file = os.environ.get('ECS_CONTAINER_METADATA_FILE')

    if metadata_file:
        logger.debug(f'The container metadata file location is {metadata_file}')
    else:
        logger.warning('The container metadata file location is unknown')
        return {}

    def do_read():
        with open(metadata_file, 'r') as f:
            return json.load(f)

    try:
        metadata = do_read()
    except FileNotFoundError:
        logger.warning(f'The container metadata file cannot be read from {metadata_file}')
        return {}

    start = datetime.datetime.now()
    while wait_for_ready and metadata.get('MetadataFileStatus') != 'READY':
        if (datetime.datetime.now() - start).total_seconds() >= (timeout_sec or 60):
            raise TimeoutError("Timed out while waiting for metadata file READY status")
        time.sleep(1)
        metadata = do_read()
    return metadata


def check_is_instance_draining(container_metadata):
    """
    Determines if the EC2 instance on which an ECS container is running is in a DRAINING state.
    See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/container-instance-draining.html

    Args:
        ecs_client: An instance of the boto3 client for the ECS service
        container_metadata: The container metadata describing the instance that should be inspected

    Returns:
        bool: True if the EC2 instance on which an ECS container is running is in a DRAINING state.
    """
    if container_metadata:
        response = __get_ecs_client().describe_container_instances(
            cluster=container_metadata.get('Cluster'),
            containerInstances=[container_metadata.get('ContainerInstanceARN')]
        )

        if response['containerInstances'][0]['status'] == 'DRAINING':
            return True

    return False

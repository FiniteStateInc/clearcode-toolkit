import boto3
import os
import re
import uuid

from datetime import datetime
from typing import List
from urllib.parse import urlparse

from finitestate.firmware.plugins.messages import FSFirmwareUnpackedMessage

sqs_client = None


def get_sqs_client():
    global sqs_client
    if not sqs_client:
        sqs_client = boto3.get_client('sqs')
    return sqs_client


def construct_file_tree(package_hash: str, harvest_data: dict, scancode_data: dict) -> List[dict]:
    """ """

    """
    Construct a dictionary of the form
    {
        <path>: {
            'harvest_info': {...},
            'scancode_info': {...}
        },
        ... other files keyed by path ...
    }
    From which the file tree can be made
    """

    transformed_data = dict()
    for file_entry in harvest_data['files']:
        transformed_data[file_entry['path']] = {
            'harvest_info': file_entry,
            'scancode_info': dict()
        }

    for file_entry in scancode_data['content']['files']:
        # We do not want to think about directories.
        if file_entry['type'] == 'file':
            if file_entry['path'] not in transformed_data:
                transformed_data[file_entry['path']] = {
                    'harvest_info': dict(),
                    'scancode_info': file_entry
                }
            else:
                transformed_data[file_entry['path']]['scancode_info'] = file_entry


    # Construct the file tree using the transformed data
    file_tree = list()

    # Add the root file for the package itself

    file_tree.append({
        'firmware_hash': package_hash,
        'file_hash': package_hash,
        'file_full_path': '/',
        'file_name': '',
        # FIXME: We don't know the size of the compressed package. This would
        # be nice to have, but we're not sure where that comes from.
        'file_size': 0,
        # FIXME
        # The package_data we're passing in right now does not include
        # the mime-type. Hand-wave and say that it's text/plain regardless
        # of what we're looking at.
        # The mime type information can be found in the scancode document
        # for a given package.
        'file_type_mime': 'text/plain',
        'file_type_full': 'ClearlyDefined Unknown'
    })

    # Add entries for each file in the package
    for path, info in transformed_data.items():
        # In the ClearlyDefined dataset, all paths are relative
        # with the root appearing as cwd. Instead, we should add a /
        # to the start of each of the dirents so that it conforms to
        # finitestate standards where the top level of the extracted
        # package is root.
        file_path = f"/{path}"
        file_tree.append(
            {
                'firmware_hash': package_hash,
                'file_hash': info.get('harvest_info', dict()).get('hashes', dict()).get('sha256', None),
                'file_full_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': info.get('scancode_info', dict()).get('size', 0),
                # We don't want to accidentally process anything as a binary who is not.
                'file_type_mime': info.get('scancode_info', dict()).get('mime_type', 'text/plain'),
                # Make sure we can query for these, if need be.
                'file_type_full': info.get('scancode_info', dict()).get('file_type', 'ClearlyDefined Unknown'),
            }
        )

    return file_tree


def construct_ground_truth_upload_metadata(package_hash: str, package_data: dict) -> dict:

    package_url = package_data['registryData']['manifest']['dist']['tarball']
    package_filename = os.path.basename(package_url)
    package_url_parsed = urlparse(package_url)
    supplier_base_url = f'{package_url_parsed.scheme}://{package_url_parsed.netloc}/'

    return {
        'additional_metadata': {
            'upload_date': package_data['registryData']['releaseDate'],
            'license': package_data['registryData']['license'],
            'project_name': package_data['registryData']['name'],
            'home_page': package_data['registryData']['homepage'],
            'file_count': package_data['registryData']['manifest']['dist']['fileCount'],
            'description': package_data['registryData']['manifest']['description'],
            'package_version': package_data['registryData']['manifest']['version']
        },
        'download_date': str(datetime.utcnow()),
        'download_location': package_url,
        'download_type': "clearlydefined-harvest",
        'download_method': "clearlydefined-scraper-npm",
        'file_name': package_filename,
        'supplier_base_url': supplier_base_url,
        'supplier_name': "npm",
        'file_hash': package_hash
    }


def get_package_json(file_tree: dict, data: dict):
    package_json_sha256_digest = str()
    for file in file_tree:
        # Look for a package.json either exactly at the root of the package
        # or a single directory down.
        # This regex captures paths of the form
        # /package.json
        # /arbitrary-string/package.json
        # and nothing else.
        if re.match(r'^(\/[^\/]*)?\/package.json', file['file_full_path']):
            package_json_sha256_digest = file['file_hash']

    package_json = data["package.json"]  # _string: str = json.dumps(data["package.json"], indent=True)

    return {
        'package_json_hash': package_json_sha256_digest,
        'package_json': package_json
    }


PLUGIN_NAME = 'package_metadata'
STAGE = 'dev2'
ACCOUNT_ID = '185231689230'
REGION = 'us-east-1'
PRIORITY = 'low'


def trigger_package_metadata_plugin(package_hash: str):
    queue_url = f'https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/firmware-{STAGE}-{PLUGIN_NAME}-{PRIORITY}'
    message = FSFirmwareUnpackedMessage(firmware_id=package_hash, fwan_process_id=str(uuid.uuid4()), trigger_downstream_plugins=True)
    print(
        f"SQS Message to send: QueueUrl: {queue_url}, MessageBody: {message.serialize()}"
    )
    get_sqs_client().send_message(
        QueueUrl=queue_url,
        MessageBody=message.serialize()
    )


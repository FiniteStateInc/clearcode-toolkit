import boto3
import json
import os
import re
import uuid

from abc import ABC
from datetime import datetime
from typing import List
from urllib.parse import urlparse

from finitestate.firmware.aws.s3 import upload_data_to_s3
from finitestate.firmware.plugins.messages import FSFirmwareUnpackedMessage
from finitestate.firmware.plugins.storage import FSAWSStorageAdapter


STAGE = 'dev2'
ACCOUNT_ID = '185231689230'
FILE_BUCKET = f"finitestate-firmware-{STAGE}-files"
METADATA_BUCKET = f"finitestate-firmware-{STAGE}-metadata"
PLUGIN_NAME = 'package_metadata'
PRIORITY = 'low'
REGION = 'us-east-1'

sqs_client = None
storage_adapter = None


def get_storage_adapter():
    global storage_adapter
    if not storage_adapter:
        storage_adapter = FSAWSStorageAdapter(FILE_BUCKET, METADATA_BUCKET)
    return storage_adapter


def get_sqs_client():
    global sqs_client
    if not sqs_client:
        sqs_client = boto3.get_client('sqs')
    return sqs_client


class HarvestParser(ABC):
    def __init__(self, package_hash: str, clearlydefined_data: dict, scancode_data: dict):
        self.package_hash = package_hash
        self.clearlydefined_data = clearlydefined_data
        self.scancode_data = scancode_data
        self.storage_adapter = get_storage_adapter()

    def _construct_file_tree(self) -> List[dict]:
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
        for file_entry in self.clearlydefined_data['files']:
            transformed_data[file_entry['path']] = {
                'harvest_info': file_entry,
                'scancode_info': dict()
            }

        for file_entry in self.scancode_data['content']['files']:
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
            'firmware_hash': self.package_hash,
            'file_hash': self.package_hash,
            'file_full_path': '/',
            'file_name': '',
            # FIXME: We don't know the size of the compressed package. This would
            # be nice to have, but we're not sure where that comes from.
            'file_size': 0,
            # FIXME
            # The clearlydefined_data we're passing in right now does not include
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
                    'firmware_hash': self.package_hash,
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

    def _construct_ground_truth_upload_metadata(self) -> dict:

        package_url = self.clearlydefined_data['registryData']['manifest']['dist']['tarball']
        package_filename = os.path.basename(package_url)
        package_url_parsed = urlparse(package_url)
        supplier_base_url = f'{package_url_parsed.scheme}://{package_url_parsed.netloc}/'

        return {
            'additional_metadata': {
                'upload_date': self.clearlydefined_data['registryData']['releaseDate'],
                'license': self.clearlydefined_data['registryData']['license'],
                'project_name': self.clearlydefined_data['registryData']['name'],
                'home_page': self.clearlydefined_data['registryData']['homepage'],
                'file_count': self.clearlydefined_data['registryData']['manifest']['dist']['fileCount'],
                'description': self.clearlydefined_data['registryData']['manifest']['description'],
                'package_version': self.clearlydefined_data['registryData']['manifest']['version']
            },
            'download_date': str(datetime.utcnow()),
            'download_location': package_url,
            'download_type': "clearlydefined-harvest",
            'download_method': "clearlydefined-scraper-npm",
            'file_name': package_filename,
            'supplier_base_url': supplier_base_url,
            'supplier_name': "npm",
            'file_hash': self.package_hash
        }

    def _trigger_package_metadata_plugin(self):
        queue_url = f'https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/firmware-{STAGE}-{PLUGIN_NAME}-{PRIORITY}'
        message = FSFirmwareUnpackedMessage(firmware_id=self.package_hash,
                                            fwan_process_id=str(uuid.uuid4()),
                                            trigger_downstream_plugins=True)
        print(
            f"SQS Message to send: QueueUrl: {queue_url}, MessageBody: {message.serialize()}"
        )
        get_sqs_client().send_message(
            QueueUrl=queue_url,
            MessageBody=message.serialize()
        )

    def parse(self):
        file_tree: List[dict] = self._construct_file_tree(self.package_hash, self.clearlydefined_data, self.scancode_data)
        ground_truth_upload_metadata: dict = self._construct_ground_truth_upload_metadata(self.package_hash, self.clearlydefined_data)

        storage_adapter.store_metadata(file_id=self.package_hash, output_location="file_tree", result=file_tree)

        storage_adapter.store_metadata_bytes(file_id=self.package_hash,
                                             output_location="ground_truth_upload_metadata",
                                             data=json.dumps(ground_truth_upload_metadata))


class NPMHarvestParser(HarvestParser):
    def __init__(self, package_hash: str, clearlydefined_data: dict, scancode_data: dict):
        super().__init__(package_hash, clearlydefined_data, scancode_data)

    def _get_package_json(self):
        package_json_sha256_digest = str()
        clearlydefined_files = self.clearlydefined_data['files']
        for file in clearlydefined_files:
            # Look for a package.json either exactly at the root of the package
            # or a single directory down.
            # This regex captures paths of the form
            # /package.json
            # /arbitrary-string/package.json
            # and nothing else.
            if re.match(r'^([^\/]*)?\/package\.json', file['path']):
                package_json_sha256_digest = file['hashes']['sha256']

        package_json = self.clearlydefined_data["package.json"]  # _string: str = json.dumps(data["package.json"], indent=True)

        return {
            'package_json_hash': package_json_sha256_digest,
            'package_json': package_json
        }

    def parse(self):
        # Do everything defined in ABC
        super().parse()
        package_json_data: dict = self._get_package_json()
        upload_data_to_s3(bucket=FILE_BUCKET,
                          key=package_json_data['package_json_hash'],
                          data=json.dumps(package_json_data['package_json'], indent=True))

        self.trigger_package_metadata_plugin()


def get_harvest_parser(package_type: str, package_hash: str, clearlydefined_data: dict, scancode_data: dict) -> HarvestParser:
    if package_type == 'npm':
        return NPMHarvestParser(package_hash, clearlydefined_data, scancode_data)
    else:
        raise NotImplementedError(f"Harvest Parser not implemented for package type {package_type}")

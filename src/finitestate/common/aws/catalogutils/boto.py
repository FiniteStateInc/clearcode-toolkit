from .core import DataCatalogUtils

import os
import boto3
from botocore.exceptions import ClientError


class BotoDataCatalogUtils(DataCatalogUtils):
    def __init__(self, region_name=None):
        self.glue_client = boto3.client('glue', region_name=region_name or os.environ.get('AWS_DEFAULT_REGION'), endpoint_url=os.environ.get('GLUE_ENDPOINT_URL'))

    def get_table_path(self, database_name, table_name):
        table = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
        return table['Table']['StorageDescriptor']['Location']

    def get_partition_path(self, database_name, table_name, **partition_kwargs):
        table = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)

        partition_keys = table['Table']['PartitionKeys']
        partition_values = [str(partition_kwargs[k['Name']]) for k in partition_keys]

        partition = self.glue_client.get_partition(DatabaseName=database_name, TableName=table_name, PartitionValues=partition_values)

        return partition['Partition']['StorageDescriptor']['Location']

    def get_table_properties(self, database_name, table_name):
        table = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
        return table['Table'].get('Parameters', {})

    def check_partition_exists(self, database_name, table_name, **partition_kwargs):
        table = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)

        partition_keys = table['Table']['PartitionKeys']
        partition_values = [str(partition_kwargs[k['Name']]) for k in partition_keys]

        try:
            partition = self.glue_client.get_partition(DatabaseName=database_name, TableName=table_name, PartitionValues=partition_values)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == 'EntityNotFoundException':
                return False
            raise e

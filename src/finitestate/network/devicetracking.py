import datetime
import os
import time
import typing
import uuid
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, greatest, least, lit, max as _max, min as _min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import finitestate.common.aws.s3 as fss3


# yapf: disable
def get_device_tracking_observations(df):
    """
    Calculates first_observed_at and last_observed_at for each device from the supplied observations.

    :param df: A DataFrame of network connection observations.
    :return: A DataFrame of (organization, mac, first_observed_at, last_observed_at)
    """

    if df and df.columns:
        outbound_df = df.select(
            'organization',
            col('network_src_mac').name('mac'),
            'occurred_at'
        )

        inbound_df = df.select(
            'organization',
            col('network_dest_mac').name('mac'),
            'occurred_at'
        )

        return outbound_df.union(
            inbound_df
        ).where(
            col('mac').isNotNull()
        ).groupBy(
            'organization',
            'mac'
        ).agg(
            _min('occurred_at').name('first_observed_at'),
            _max('occurred_at').name('last_observed_at'),
        )


def amend_device_tracking(observations_df, tracking_df, last_updated_by):  # type: (DataFrame, DataFrame, str) -> typing.Tuple[DataFrame, DataFrame, DataFrame]
    """
    Blends new observations into an existing device tracking dataset.

    :param observations_df: New observations to be used for amending the device tracking data set.
    :param tracking_df: The device tracking data set.
    :param last_updated_by: The last updated user/process tracking field
    :return: A 3-tuple of (modified device tracking records only, updated full device tracking data set, device tracking records for never-before-seen devices only)
    """
    observations_df = observations_df.alias('o')
    tracking_df = tracking_df.alias('t')

    pk = ['organization', 'mac']

    # Find the tracking records that are changed by the new observations.

    delta_df = observations_df.select(
        'organization',
        'mac',
        'first_observed_at',
        'last_observed_at'
    ).join(
        tracking_df,
        on=pk,
        how='left'
    ).where(
        col('t.mac').isNull()
        | (col('o.first_observed_at') < col('t.first_observed_at'))
        | (col('o.last_observed_at') > col('t.last_observed_at'))
    ).select(
        'o.organization',
        'o.mac',
        least('o.first_observed_at', 't.first_observed_at').name('first_observed_at'),
        greatest('o.last_observed_at', 't.last_observed_at').name('last_observed_at'),
    ).cache()

    # Create a new version of the entire device tracking dataset, and checkpoint it to break the cyclic lineage
    # caused by reading from and writing to the same table.

    refresh_df = tracking_df.join(
        delta_df,
        on=pk,
        how='left_anti'  # Retain only the unmodified records.
    ).unionByName(
        delta_df.select(
            '*',
            current_timestamp().name('last_updated_at'),
            lit(last_updated_by).name('last_updated_by')
        )
    ).coalesce(
        1
    ).checkpoint(
        eager=True
    )

    # Find any never-before-seen devices.

    new_devices_df = delta_df.join(
        tracking_df,
        on=pk,
        how='left_anti'
    ).cache()

    return delta_df, refresh_df, new_devices_df


def atomic_refresh_device_tracking(glue_client, refresh_df, database, table, sub_path=None, remove_aged_output_thresh_time_delta=datetime.timedelta(days=1)):
    """
    This process atomically replaces the device tracking table by writing to a new directory and then updating
    the Glue Data Catalog table storage location.

    This function depends on the device_tracking table having the fs_base_location parameter which defines the
    root directory under which all of this directory shuffling occurs.

    :param glue_client: A boto3 glue client
    :param refresh_df: The dataframe will refreshed device tracking data
    :param database: The target glue database name
    :param table: The device tracking table name
    :param sub_path: (Optional) The sub_path under the table's base location to which the new output will be written.  Should be unique - e.g. a Glue Job Run ID; defaults to a new UUID if not supplied.
    :param remove_aged_output_thresh_time_delta: (Optional) Aged device tracking data at least as old as UTC now - this value will be deleted
    """

    table_desc = glue_client.get_table(DatabaseName=database, Name=table)['Table']
    base_location = table_desc['Parameters']['fs_base_location']

    if not base_location:
        raise ValueError('The table must have the fs_base_location property')

    prior_path = table_desc['StorageDescriptor']['Location']
    output_path = os.path.join(base_location, sub_path or str(uuid.uuid4()))

    refresh_df.write.parquet(path=output_path, compression='snappy', mode='overwrite')

    table_desc['StorageDescriptor']['Location'] = output_path

    # Remove the table metadata entries that aren't valid for 'update_table'

    for key in ['CreatedBy', 'DatabaseName', 'CreateTime', 'UpdateTime', 'IsRegisteredWithLakeFormation']:
        table_desc.pop(key, None)

    glue_client.update_table(DatabaseName=database, TableInput=table_desc)

    # Remove older generations of the device tracking output, making sure that we
    # don't include the prior or current output regardless of the older_than threshold, so that
    # any Spark/Glue readers have time to wrap up operations that may be using the prior data set.

    if remove_aged_output_thresh_time_delta:
        _, prior_key = fss3.get_bucket_and_key_from_uri(prior_path)
        _, current_key = fss3.get_bucket_and_key_from_uri(output_path)

        aged_objects = fss3.find_aged_objects(
            base_uri=base_location,
            older_than=remove_aged_output_thresh_time_delta,
            excluded_key_prefixes=[prior_key, current_key]
        )

        if aged_objects:
            for o in aged_objects:
                o.delete()


def get_device_tracking_schema():
    """
    Returns a Spark schema for the device tracking table.
    :rtype: pyspark.sql.StructType
    """

    return StructType(fields=[
        StructField('organization', IntegerType()),
        StructField('mac', StringType()),
        StructField('first_observed_at', TimestampType()),
        StructField('last_observed_at', TimestampType()),
        StructField('last_updated_at', TimestampType()),
        StructField('last_updated_by', StringType()),
    ])


def get_dynamo_row_formatter(job_id, job_name, job_git_commit_hash, job_execution_start_time):
    def formatter(row):
        return {
            'aggregate_id': '{}_{}'.format(row['organization'], row['mac']),
            'job_id': job_id,
            'job_name': job_name,
            'job_git_commit_hash': job_git_commit_hash,
            'job_execution_start_time': Decimal(job_execution_start_time),
            'first_observed_at': Decimal(float(str(time.mktime(row['first_observed_at'].timetuple())))),
            'last_observed_at': Decimal(float(str(time.mktime(row['last_observed_at'].timetuple())))),
        }

    return formatter

# yapf: enable
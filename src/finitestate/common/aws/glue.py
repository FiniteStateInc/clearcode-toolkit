import json
import os
from typing import List

from pyspark.sql.functions import col, from_json, row_number, when
from pyspark.sql.types import BooleanType, IntegerType, StructField, StringType, StructType, TimestampType

from finitestate.common.retry_utils import retry


LAYERED = 'layered'
FLAT    = 'flat'


def format_push_down_predicate(target_time, style):
    if style == LAYERED:
        return "(year = '{}' and month = '{}' and day == '{}' and hour == '{}')".format(
            target_time.year, target_time.month, target_time.day, target_time.hour
        )
    elif style == FLAT:
        return "(dt = '{:04d}{:02d}{:02d}{:02d}')".format(
            target_time.year, target_time.month, target_time.day, target_time.hour
        )
    else:
        raise Exception("unrecognized partitioning style: " + style)


def read_dynamo_table(gc, name, read_throughput=None, splits=None):
    """
    Reads a Dynamo table as a Glue DynamicFrame.

    :param awsglue.context.GlueContext gc: The GlueContext
    :param str name: The name of the Dynamo table
    :param str read_throughput: Optional read throughput - supports values from "0.1" to "1.5", inclusive.
    :param str splits: Optional number of input splits - defaults to the SparkContext default parallelism.
    :rtype: awsglue.dynamicframe.DynamicFrame
    """

    connection_options = {
        'dynamodb.input.tableName': name,
        'dynamodb.splits': str(splits or gc.spark_session.sparkContext.defaultParallelism)
    }

    if read_throughput:
        connection_options['dynamodb.throughput.read.percent'] = str(read_throughput)

    return gc.create_dynamic_frame_from_options(connection_type='dynamodb', connection_options=connection_options)


def first_over_window(df, w):
    """
    Returns a filtered version of the supplied DataFrame where only the first record over the window is present.

    :param pyspark.sql.DataFrame df: The DataFrame to filter
    :param pyspark.sql.Window w: The analytical window over which the first row should be retained
    :rtype: pyspark.sql.DataFrame
    """
    n = '__keep__'
    return df.withColumn(n, when(row_number().over(w) == 1, True).otherwise(False)).where(col(n)).drop(n)


class ExecutorGlobals(object):
    """
    This class is meant for use in Python closures executed on PySpark executors (functions executed by map,
    foreach, etc.), and it establishes shared, reusable access to expensive resources like boto3 clients.

    The AWS_DEFAULT_REGION environment variable must be set on the workers,
    e.g. spark.config('spark.executorEnv.AWS_DEFAULT_REGION', args['AWS_REGION'])

    If any resource or client should use a custom endpoint, the value is expected to be provided in an environment
    variable matching the upper-cased name of the boto3 service followed by _ENDPOINT_URL,
    for example DYNAMODB_ENDPOINT_URL.

    One can also set a universal endpoint with DEFAULT_ENDPOINT_URL, such as for unit testing.
    """

    __dynamo_res = None
    __sns_client = None
    __s3_client = None
    __redisbloom_client = {}

    @staticmethod
    def _get_endpoint_url(service_name):
        for var in ('{}_ENDPOINT_URL'.format(prefix) for prefix in [service_name.upper(), 'DEFAULT']):
            val = os.environ.get(var)
            if val:
                return val

    @classmethod
    def dynamo_resource(cls):
        if not cls.__dynamo_res:
            import boto3
            cls.__dynamo_res = boto3.resource('dynamodb', endpoint_url=cls._get_endpoint_url('dynamodb'))
        return cls.__dynamo_res

    @classmethod
    def sns_client(cls):
        if not cls.__sns_client:
            import boto3
            cls.__sns_client = boto3.client('sns', endpoint_url=cls._get_endpoint_url('sns'))
        return cls.__sns_client

    @classmethod
    def s3_client(cls):
        if not cls.__s3_client:
            import boto3
            cls.__s3_client = boto3.client('s3', endpoint_url=cls._get_endpoint_url('s3'))
        return cls.__s3_client

    @classmethod
    def __install_packages(cls, packages: List[str]):
        """
        Glue only supports installing pure Python packages (no C library dependencies).  This function works around
        that even though it is quite obviously a horrendous hack.
        """
        from pyspark import SparkFiles
        from subprocess import run, PIPE
        import sys

        result = run(
            [sys.executable, '-m', 'pip', 'install', '--upgrade', '--no-cache-dir', '-t', SparkFiles.getRootDirectory(),
             ' '.join(packages)], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        if result.returncode != 0:
            raise Exception(
                "pip returned {returncode}:\n{stdout}\n{stderr}".format(
                    returncode=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr
                )
            )

    @classmethod
    def redisbloom_client(cls, host: str, port: int):
        """
        Returns a redisbloom Client, installing the redisbloom package if necessary.  If/when a proper virtualenv
        setup is available on Glue and redisbloom can be pre-installed, this function will still serve to create
        as few instances of the redisbloom Client as possible (one per forked Python thread per Spark executor).

        NOTE: Intended ONLY for use on a Glue (PySpark) executor, likely as the first step of a foreachParititon statement.

        :param host: The Redis host URL
        :param port: The Redis port
        :return: An instance of redisbloom.client.Client
        """
        if not (host, port) in cls.__redisbloom_client:
            try:
                from redisbloom.client import Client
            except ImportError:
                # Only install the redisbloom package to fix the failed import
                cls.__install_packages(['redisbloom==0.4.0'])
                from redisbloom.client import Client
            cls.__redisbloom_client[(host, port)] = Client(host=host, port=port)
        return cls.__redisbloom_client[(host, port)]

    @classmethod
    def kafka_producer(cls, **kafka_producer_args):
        def get_attr_name(**kwargs):
            return 'kafka_producer_{:02x}'.format(hash(json.dumps(kwargs, default=str)))

        attr_name = get_attr_name(**kafka_producer_args)

        if not hasattr(cls, attr_name):
            from kafka import KafkaProducer

            # Retry if the broker isn't available for some reason.

            @retry(on='kafka.errors.NoBrokersAvailable')
            def create_producer():
                return KafkaProducer(**kafka_producer_args)

            # Save the newly-created Kafka producer for the duration of this Spark Executor + Python thread's existence

            setattr(cls, attr_name, create_producer())
        return getattr(cls, attr_name)


class LatestDeviceProductAssociationReader(object):
    def __init__(self, gc, dynamo_table_name):
        """
        :param awsglue.context.GlueContext gc: The GlueContext
        :param str dynamo_table_name: The name of the DynamoDB table to read
        """
        self.gc = gc
        self.dynamo_table_name = dynamo_table_name

    @classmethod
    def _get_device_product_association_type_schema(cls):
        """
        Returns the Spark schema for the fields nested within each product association type (model, category, etc.)

        :rtype StructType
        """

        return StructType([
            StructField('id', StringType()),
            StructField('name', StringType()),
            StructField('confidence', IntegerType())
        ])

    @classmethod
    def _get_device_product_association_payload_schema(cls):
        """
        Returns the Spark schema for the entire product association payload.

        :return: StructType
        """

        assoc_type_schema = cls._get_device_product_association_type_schema()
        payload_schema = StructType([
            StructField('contains_phi', BooleanType()),
            StructField('category', assoc_type_schema),
            StructField('model', assoc_type_schema),
        ])
        return payload_schema

    @classmethod
    def _parse_device_product_associations_payload(cls, df, payload_column_name='payload'):
        """
        Parses the JSON payload column containing the product association information.

        :param DataFrame df: The input DataFrame
        :param str payload_column_name: (Optional) The name of the JSON payload column - defaults to `payload`
        :rtype DataFrame
        """

        payload_schema = cls._get_device_product_association_payload_schema()

        return df \
            .withColumn(payload_column_name, from_json(payload_column_name, schema=payload_schema)) \
            .select('*', '{}.*'.format(payload_column_name)) \
            .drop(payload_column_name)

    def load_device_product_associations(self, ignore_missing_table=False):
        """
        Reads the latest best product association information for each device.

        :param bool ignore_missing_table: (Optional) If set to true, will catch and suppress any errors caused by the DynamoDB table not existing, and an empty DataFrame will be returned.
        :rtype DataFrame
        """

        from finitestate.common.py4jutil import is_py4j_error_root_cause_class
        from py4j.protocol import Py4JJavaError

        def empty_df():
            return self.gc.spark_session.createDataFrame([], schema=StructType(
                fields=[StructField('aggregate_id', StringType()),
                        StructField('product_association_occurred_at', TimestampType()),
                        StructField('snapshot_version', IntegerType())] + self._get_device_product_association_payload_schema().fields))

        df = None

        try:
            df = read_dynamo_table(
                self.gc,
                name=self.dynamo_table_name,
                read_throughput='0.1'
            ).resolveChoice(
                specs=[('occurred_at', 'cast:float')]
            ).toDF()
        except Py4JJavaError as e:
            if ignore_missing_table and is_py4j_error_root_cause_class(e, 'com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException'):
                self.gc.get_logger().warn('Device product association table is missing - proceeding without product associations')
            else:
                raise e

        if not df or not df.columns or 'payload' not in df.columns:
            return empty_df()

        df = df.select(
            col('entity_identifier').name('aggregate_id'),
            col('occurred_at').cast(TimestampType()).name('product_association_occurred_at'),
            'payload',
            col('product_association_snapshot_version').cast(IntegerType()).name('snapshot_version'),
        )

        return self._parse_device_product_associations_payload(df)

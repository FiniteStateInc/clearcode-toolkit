from .core import DataCatalogUtils


def using_boto3(region_name=None) -> DataCatalogUtils:
    """
    using_boto3 will instantiate a new BotoDataCatalogUtils, which contains an
    instantiated glue client and a host of tools for interacting with glue at a
    boto3 level. It pulls region name either from a kwargument or from environment
    if the region_name argument is null.
    """
    from .boto import BotoDataCatalogUtils
    return BotoDataCatalogUtils(region_name=region_name)


def using_spark(spark=None) -> DataCatalogUtils:
    """
    using_spark will instantiate a new SparkDataCatalogUtils, which
    contains an instantiated SparkSession who is either created or got from the current
    environment. Similar to the BotoDataCatalogUtils, it contains a host of tools for
    interacting with spark. If the spark kwargument is left as null, the SparkDataCatalogUtils
    will instantiate a Spark Session for you.
    """
    from .spark import SparkDataCatalogUtils
    return SparkDataCatalogUtils(spark)

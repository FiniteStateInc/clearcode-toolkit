class DataCatalogUtils(object):
    """
    DataCatalogUtils is a base class for interacting with spark, glue,
    or another service of the like. It is extensible and currently used
    in the construction of SparkDataCatalogUtils and BotoDataCatalogUtils.
    """

    def get_table_path(self, database_name, table_name):
        raise NotImplementedError()

    def get_partition_path(self, database_name, table_name, **partition_kwargs):
        raise NotImplementedError()

    def get_table_properties(self, database_name, table_name):
        raise NotImplementedError()

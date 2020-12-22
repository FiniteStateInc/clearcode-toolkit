from .core import DataCatalogUtils

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class SparkDataCatalogUtils(DataCatalogUtils):
    def __init__(self, spark = None):
        self.spark = spark or SparkSession.builder.enableHiveSupport().getOrCreate()

    def _describe_extended(self, database_name, table_name):
        query = 'describe extended {database_name}.{table_name}'.format(
            database_name=database_name, table_name=table_name
        )
        return self.spark.sql(query)

    def get_table_path(self, database_name, table_name):
        return self._describe_extended(database_name, table_name) \
            .where("col_name = 'Location'").select(trim(col("data_type"))).collect()[0][0]

    def get_partition_path(self, database_name, table_name, **partition_kwargs):
        # Find the ordered definition of the partition columns for the table.

        desc_rows = self.spark.sql("desc network_prod.normalized_conn").collect()
        start_part_cols = next(idx for idx, row in enumerate(desc_rows) if row.col_name.strip() == '# col_name') + 1

        # Build the partition spec

        part_spec_values = []

        for part_col in desc_rows[start_part_cols:]:
            if part_col.data_type == 'str':
                part_val_str = "'{value}'".format(value=partition_kwargs[part_col.col_name])
            else:
                part_val_str = str(partition_kwargs[part_col.col_name])

            part_spec_values.append(part_col.col_name + '=' + part_val_str)

        if not part_spec_values:
            raise ValueError('The table is not partitioned or extraction of partition columns failed')

        query = 'describe extended {database_name}.{table_name} partition({spec})'.format(
            database_name=database_name, table_name=table_name, spec=','.join(part_spec_values)
        )
        return self.spark.sql(query).where("col_name = 'Location'").select(trim(col("data_type"))).collect()[0][0]

    def get_table_properties(self, database_name, table_name):
        props = self._describe_extended(database_name, table_name) \
            .where("col_name = 'Table Properties'").select(trim(col("data_type"))).collect()[0][0]
        # Slice to remove embedded brackets, then split properties by , and key/value by =
        return dict(p.strip().split('=') for p in props[1:-1].split(','))


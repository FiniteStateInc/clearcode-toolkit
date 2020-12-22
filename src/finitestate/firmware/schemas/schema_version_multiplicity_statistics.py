import pyspark.sql.types

version_multiplicity_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('software_version_multiplicity', pyspark.sql.types.IntegerType()),
])

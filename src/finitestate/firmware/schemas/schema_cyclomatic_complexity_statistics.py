import pyspark.sql.types

cyclomatic_complexity_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('max_complexity', pyspark.sql.types.IntegerType()),
])

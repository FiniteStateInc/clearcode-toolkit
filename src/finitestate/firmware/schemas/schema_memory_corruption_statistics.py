import pyspark.sql.types

memory_corruption_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('memory_corruption_binary_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('memory_corruption_count', pyspark.sql.types.IntegerType()),
])

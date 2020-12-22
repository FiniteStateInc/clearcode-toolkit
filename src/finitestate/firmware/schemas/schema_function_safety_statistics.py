import pyspark.sql.types

function_safety_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('safe_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('unsafe_count', pyspark.sql.types.IntegerType()),
])

import pyspark.sql.types
    
bin_info_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('relro_full_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('aslr', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('dep', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('stackguard', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('count_with_security_features', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('total_bin_info_info_count', pyspark.sql.types.IntegerType()),
])


import pyspark.sql.types
    
code_security_ratios_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('relro_full_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('dep_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('aslr_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('stackguard_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('binaries_in_firmware', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('count_with_security_features', pyspark.sql.types.IntegerType()),
])


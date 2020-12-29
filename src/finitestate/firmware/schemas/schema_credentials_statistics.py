import pyspark.sql.types

credentials_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('creds_from_shadow', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('creds_from_passwd', pyspark.sql.types.IntegerType()),
])

import pyspark.sql.types

shadow_parse_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('user', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('password', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('last_changed', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('min_days', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('max_days', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('warn_days', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('inactive_days', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('expires', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('flag', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])

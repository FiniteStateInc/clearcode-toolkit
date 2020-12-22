import pyspark.sql.types

passwd_parse_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('user', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('password', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('uid', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('guid', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('full_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('home_dir', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('shell', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])

import pyspark.sql.types

code_detect_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('file_full_path', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('language', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])


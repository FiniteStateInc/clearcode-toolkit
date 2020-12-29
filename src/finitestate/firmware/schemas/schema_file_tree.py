import pyspark.sql.types

file_tree_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_full_path', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_type_mime', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_type_full', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_size', pyspark.sql.types.LongType()),
])

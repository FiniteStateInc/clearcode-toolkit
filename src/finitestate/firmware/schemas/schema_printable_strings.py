import pyspark.sql.types
    
printable_strings_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('string', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('offset', pyspark.sql.types.LongType()),
])


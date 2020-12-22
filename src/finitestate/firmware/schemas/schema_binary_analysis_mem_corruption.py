import pyspark.sql.types
    
binary_analysis_mem_corruption_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('func_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('func_addr', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('containing_func_addr', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('containing_func_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('src_size', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('dest_size', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('confidence', pyspark.sql.types.StringType()),
])


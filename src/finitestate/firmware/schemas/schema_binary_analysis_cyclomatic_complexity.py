import pyspark.sql.types
    
binary_analysis_cyclomatic_complexity_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('func_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('complexity', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])


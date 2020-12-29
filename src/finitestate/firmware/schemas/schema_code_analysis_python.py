import pyspark.sql.types
    
code_analysis_python_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('code', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('issue_confidence', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('issue_severity', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('issue_text', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('line_number', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('line_range', pyspark.sql.types.ArrayType(pyspark.sql.types.IntegerType())),
    pyspark.sql.types.StructField('more_info', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('test_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])


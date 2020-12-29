import pyspark.sql.types

binary_analysis_function_info_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('func_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('func_address', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('xref_to_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('xrefs_from', pyspark.sql.types.ArrayType(pyspark.sql.types.StringType())),
    pyspark.sql.types.StructField('xrefs_to', pyspark.sql.types.ArrayType(pyspark.sql.types.StringType())),
])

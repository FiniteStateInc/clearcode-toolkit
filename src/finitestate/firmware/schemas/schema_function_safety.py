import pyspark.sql.types
    
function_safety_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('type', pyspark.sql.types.StringType()),
])


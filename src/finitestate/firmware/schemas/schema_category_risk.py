import pyspark.sql.types
    
category_risk_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('category', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('risk_score', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('created_at', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('updated_at', pyspark.sql.types.StringType()),
])


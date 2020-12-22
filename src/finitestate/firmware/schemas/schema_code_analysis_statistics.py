import pyspark.sql.types

code_analysis_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('weighted_file_risk', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('high_severity_high_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('high_severity_medium_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('high_severity_low_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('medium_severity_high_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('medium_severity_medium_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('medium_severity_low_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('low_severity_high_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('low_severity_medium_confidence', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
])

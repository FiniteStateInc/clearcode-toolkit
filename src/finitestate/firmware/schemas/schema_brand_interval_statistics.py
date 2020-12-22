import pyspark.sql.types
    
brand_interval_statistics_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('brand', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('code_analysis_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('code_security_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('creds_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('crypto_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('code_complexity_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('cve_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('safe_calls_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('memory_corruptions_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('software_version_multiplicity_interval_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('brand_composite_score', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('b64brand', pyspark.sql.types.StringType()),
])


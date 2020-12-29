import pyspark.sql.types

device_vulnerabilities_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('aggregate_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('brand_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('category_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('document_type', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('cpe_count', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('severity', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('LOW', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('MEDIUM', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('HIGH', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('CRITICAL', pyspark.sql.types.LongType()),
    ])),
])
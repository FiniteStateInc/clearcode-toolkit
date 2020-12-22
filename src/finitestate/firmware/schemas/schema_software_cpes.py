import pyspark.sql.types

software_cpes_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('cpe', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('evidence', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('type', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('product', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('version', pyspark.sql.types.StringType()),
    ])),
])

import pyspark.sql.types


sbom_cpes_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('cpe', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('version', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('description', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('short', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('long', pyspark.sql.types.StringType()),
    ])),
    pyspark.sql.types.StructField('purl', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('evidence', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('cpe', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('product', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('version', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('description', pyspark.sql.types.StructType([
            pyspark.sql.types.StructField('short', pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField('long', pyspark.sql.types.StringType()),
        ])),
        pyspark.sql.types.StructField('purl', pyspark.sql.types.StringType()),
    ])),
])


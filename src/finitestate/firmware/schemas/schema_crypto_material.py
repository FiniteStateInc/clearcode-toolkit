import pyspark.sql.types

crypto_material_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('key_size', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('material', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('algorithm', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('expiration', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('private_key', pyspark.sql.types.BooleanType()),
    pyspark.sql.types.StructField('material_type', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('issuer', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('locality', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('raw_issuer', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('common_name', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('country_name', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('organization', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('organizational_unit', pyspark.sql.types.StringType()),
        pyspark.sql.types.StructField('state_or_province_name', pyspark.sql.types.StringType()),
    ]))
])

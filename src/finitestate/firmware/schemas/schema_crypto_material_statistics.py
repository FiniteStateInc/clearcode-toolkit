import pyspark.sql.types

crypto_material_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('ssh_rsa_private_key_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('ssh_rsa_public_key_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('host_keys_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('authorized_keys_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('pgp_private_key_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('pkcs8_private_key_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('pkcs12_certificate_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('ssl_private_key_count', pyspark.sql.types.IntegerType())
])

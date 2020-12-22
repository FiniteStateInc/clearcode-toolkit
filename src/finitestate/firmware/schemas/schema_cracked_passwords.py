import pyspark.sql.types

cracked_passwords_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('password_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('cracked_password', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('cracker_log', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
])

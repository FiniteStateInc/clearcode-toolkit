import pyspark.sql.types

validated_vulnerabilities_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('cve_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('severity', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('published', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('summary', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('cpes', pyspark.sql.types.ArrayType(pyspark.sql.types.StringType()))
])


import pyspark.sql.types

software_components_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('open_source', pyspark.sql.types.BooleanType()),
    pyspark.sql.types.StructField('website', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('description', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('version', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('file_paths', pyspark.sql.types.ArrayType(pyspark.sql.types.StringType()))])

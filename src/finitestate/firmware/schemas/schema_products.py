import pyspark.sql.types
    
products_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_sha256', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_model_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_model_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_brand_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_family_name', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_model_firmware_version', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_model_firmware_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_brand_id', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('product_categories', pyspark.sql.types.ArrayType(pyspark.sql.types.StringType())),
])


import pyspark.sql.types

vxworks_security_ratios_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('interrupt_stack_protection_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('kernel_stack_protection_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('password_protection_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('user_task_stack_protection_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('write_protection_percent', pyspark.sql.types.DoubleType()),
    pyspark.sql.types.StructField('vxworks_binary_count', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('count_with_security_features', pyspark.sql.types.IntegerType()),
])
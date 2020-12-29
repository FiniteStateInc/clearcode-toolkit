import pyspark.sql.types

vxworks_safety_features_statistics_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('write_protection', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('password_protection', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('total_vxworks_count', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('kernel_stack_protection', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('interrupt_stack_protection', pyspark.sql.types.LongType()),
    pyspark.sql.types.StructField('user_task_stack_protection', pyspark.sql.types.LongType()),
])
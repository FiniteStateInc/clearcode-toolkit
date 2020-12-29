import pyspark.sql.types

vxworks_safety_features_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('file_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('vxworks_version', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('write_protection', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('kernel_text', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('user_text', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('virtual_mem_text', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('vector_table', pyspark.sql.types.BooleanType()),
    ])),
    pyspark.sql.types.StructField('interrupt_stack_protection', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('guard_zones', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('guard_overflow_size', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_underflow_size', pyspark.sql.types.LongType())
    ])),
    pyspark.sql.types.StructField('kernel_stack_protection', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('guard_overflow_size_exec', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_underflow_size_exec', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_overflow_size_exception', pyspark.sql.types.LongType())
    ])),
    pyspark.sql.types.StructField('password_protection', pyspark.sql.types.BooleanType()),
    pyspark.sql.types.StructField('user_task_stack_protection', pyspark.sql.types.StructType([
        pyspark.sql.types.StructField('no_exec', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('guard_zones', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('global_stack_fill', pyspark.sql.types.BooleanType()),
        pyspark.sql.types.StructField('check_stack_xrefs', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_overflow_size_exec', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_underflow_size_exec', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('guard_overflow_size_exception', pyspark.sql.types.LongType()),
        pyspark.sql.types.StructField('task_specific_info', pyspark.sql.types.ArrayType(
                pyspark.sql.types.StructType([
                    pyspark.sql.types.StructField('name', pyspark.sql.types.StringType()),
                    pyspark.sql.types.StructField('stack_fill', pyspark.sql.types.BooleanType()),
                    pyspark.sql.types.StructField('guard_zones', pyspark.sql.types.BooleanType()),
                ])
            )
        )
    ]))
])

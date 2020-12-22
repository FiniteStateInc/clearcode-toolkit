import pyspark.sql.types


sbom_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('created_at', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('components', pyspark.sql.types.ArrayType(
        pyspark.sql.types.StructType([
            pyspark.sql.types.StructField('name', pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField('status', pyspark.sql.types.ArrayType(
                pyspark.sql.types.StringType()
            )),
            pyspark.sql.types.StructField('version', pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField('purl', pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField('description', pyspark.sql.types.StructType([
                pyspark.sql.types.StructField('short', pyspark.sql.types.StringType()),
                pyspark.sql.types.StructField('long', pyspark.sql.types.StringType()),
            ])),
            pyspark.sql.types.StructField('external_references', pyspark.sql.types.ArrayType(
                pyspark.sql.types.StructType([
                    pyspark.sql.types.StructField('url', pyspark.sql.types.StringType()),
                    pyspark.sql.types.StructField('type', pyspark.sql.types.StringType()),
                ])
            )),
            pyspark.sql.types.StructField('files', pyspark.sql.types.ArrayType(
                pyspark.sql.types.StructType([
                    pyspark.sql.types.StructField('hashes', pyspark.sql.types.StructType([
                        # May need expansion later if we add more hashes
                        pyspark.sql.types.StructField('sha256', pyspark.sql.types.StringType()),
                    ])),
                    pyspark.sql.types.StructField('names', pyspark.sql.types.ArrayType(
                        pyspark.sql.types.StringType()
                    )),
                    pyspark.sql.types.StructField('evidence', pyspark.sql.types.ArrayType(
                        pyspark.sql.types.StructType([
                            pyspark.sql.types.StructField('type', pyspark.sql.types.StringType()),
                            pyspark.sql.types.StructField('confidence_score', pyspark.sql.types.DoubleType()),
                            pyspark.sql.types.StructField('description', pyspark.sql.types.StringType()),
                        ])
                    ))
                ])
            ))
        ])
    )),
    pyspark.sql.types.StructField('relationships', pyspark.sql.types.ArrayType(
        pyspark.sql.types.StructType([
            pyspark.sql.types.StructField('source', pyspark.sql.types.StructType([
                # May need expanded if we add more sources
                pyspark.sql.types.StructField('purl', pyspark.sql.types.StringType()),
            ])),
            pyspark.sql.types.StructField('target', pyspark.sql.types.StructType([
                # May need expanded if we add more sources
                pyspark.sql.types.StructField('purl', pyspark.sql.types.StringType()),
            ])),
            pyspark.sql.types.StructField('type', pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField('description', pyspark.sql.types.StringType()),
        ])
    ))
])


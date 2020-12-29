from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import *


l3_enabled_orgs = [1024]


def has_unique_src_mac_from_ip() -> Column:
    return col('network_src_mac_from_ip').isNotNull() & (col('network_src_mac_from_ip') != col('network_src_mac'))


def has_unique_dest_mac_from_ip() -> Column:
    return col('network_dest_mac_from_ip').isNotNull() & (col('network_dest_mac_from_ip') != col('network_dest_mac'))


def resolve_macs(df: DataFrame, src: bool = True, dest: bool = True) -> DataFrame:
    if df and df.columns:
        if 'network_src_mac_from_ip' in df.columns:
            df = df.withColumn(
                '__has_unique_src_mac_from_ip__', (lit(src) & col('organization').isin(l3_enabled_orgs) & has_unique_src_mac_from_ip())
            ).withColumn(
                'network_src_mac', when(col('__has_unique_src_mac_from_ip__'), col('network_src_mac_from_ip')).otherwise(col('network_src_mac'))
            )
        else:
            df = df.withColumn('__has_unique_src_mac_from_ip__', lit(False))

        if 'network_dest_mac_from_ip' in df.columns:
            df = df.withColumn(
                '__has_unique_dest_mac_from_ip__', (lit(dest) & col('organization').isin(l3_enabled_orgs) & has_unique_dest_mac_from_ip())
            ).withColumn(
                'network_dest_mac', when(col('__has_unique_dest_mac_from_ip__'), col('network_dest_mac_from_ip')).otherwise(col('network_dest_mac'))
            )
        else:
            df = df.withColumn('__has_unique_dest_mac_from_ip__', lit(False))

        df = df.withColumn(
            'mac_address_source_layer', when((col('__has_unique_src_mac_from_ip__') | col('__has_unique_dest_mac_from_ip__')), 3).otherwise(2)
        ).drop(
            '__has_unique_src_mac_from_ip__',
            '__has_unique_dest_mac_from_ip__',
        )

    return df

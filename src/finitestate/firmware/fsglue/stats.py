from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as _sum, max as _max, lower as _lower, col, count, explode_outer, when, count, countDistinct, percent_rank
from pyspark.sql.window import Window

# General Stats
# These do some basic calculations on statistical datasets


# This will get a statistic from a dataframe. This function is meant to be used on a dataframe that
# looks like
# [Row(firmware_hash='sample', statistic_name=value)]
# with only _one_ firmware hash and _one_ statistic. the resultant statistic is to
# be fed to interpolate_quantile.
# Right now, this will _swallow exceptions_, returning 0.
def get_statistic_from_dataframe(count_dataframe: DataFrame, statistic_name: str):
    """
    :rtype typing.Union[int, float]
    """
    try:
        return count_dataframe.collect()[0][statistic_name]
    except Exception as e:
        print('Unable to get statistic from dataframe: {}'.format(e))
        return 0


# This will find an approximate quantile based on a quantile document formatted like
# {
#   1: a1,
#   2: a2,
#   ...
#   99: a99
# }
# Where ai are statistics over some dataset, the keys of the dictionaries
# are the quantiles to which those statistics belong.
def interpolate_quantile(statistic: int, quantile_document: dict) -> float:
    if not quantile_document or statistic > quantile_document[99]:
        return 99.0 / 100.0
    if statistic <= 0:
        return 0.0
    for i in range(1, 100):
        if statistic > quantile_document[i]:
            continue
        else:
            return float((i - 1)) / 100.0


def calculate_quantiles(dataframe: DataFrame, column: str, tolerance: float) -> dict:
    quantiles_list = dataframe.approxQuantile(column, [x / 100 for x in range(1, 100)], tolerance)
    keys = [x for x in range(1, 100)]
    return dict(zip(keys, quantiles_list))


# Transforms
# These take in firmware analysis dataframes and spit out dataframes containing basic statistical information

# ////\\\\//////\\\\///////\\///\///
# ////       Code Security   ///\\\/
# ////\\\\//////\\\\///////\\///\///


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# bin_info_info_dataframe: a dataframe representing bin info info, down-selected to relro, relocs, nx, canary and file hash
# Output
# bin_info_info_stats_df: a dataframe containing firmware_hash, relro_full_count, aslr, dep, stackguard, count_with_security_features and total_bin_info_info_count
def prepare_binary_info_data(file_tree_dataframe: DataFrame, bin_info_info_dataframe: DataFrame) -> DataFrame:
    # yapf: disable

    # Linter is angry about this, but this is pyspark, not python that we're dealing with
    bin_info_info_counted = bin_info_info_dataframe.select(
        when(
            col('relro') == 'full', 1
        ).otherwise(0).alias('relro_full'),
        when(
            col('relro') == 'partial', 1
        ).otherwise(0).alias('relro_partial'),
        when(
            col('relocs') == True, 1
        ).otherwise(0).alias('aslr_on'),
        when(
            col('nx') == True, 1
        ).otherwise(0).alias('dep_on'),
        when(
            col('canary') == True, 1
        ).otherwise(0).alias('stackguard_on'),
        when(
            (col('relro') == 'full') | (col('relocs') == True) | (col('canary') == True) | (col('nx') == True), 1
        ).otherwise(0).alias('has_one_security_feature'),
        'file_hash',
        when(
            col('file_hash').isNotNull(), 1
        ).otherwise(0).alias('row_count')
    )
    bin_info_info_stats_df = bin_info_info_counted.join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('relro_full').cast('int').alias('relro_full_count'),
        _sum('aslr_on').cast('int').alias('aslr'),
        _sum('dep_on').cast('int').alias('dep'),
        _sum('stackguard_on').cast('int').alias('stackguard'),
        _sum('has_one_security_feature').cast('int').alias('count_with_security_features'),
        _sum('row_count').cast('int').alias('total_bin_info_info_count')
    ).select(
        'firmware_hash',
        'relro_full_count',
        'aslr',
        'dep',
        'stackguard',
        'count_with_security_features',
        'total_bin_info_info_count'
    )
    # yapf: enable
    return bin_info_info_stats_df


def prepare_binary_info_features_per_binary(file_tree_dataframe: DataFrame, bin_info_dataframe: DataFrame) -> DataFrame:
    return bin_info_dataframe.withColumn(
        'ratio',
        (
            when(
                col('relro') == 'full', 1
            ).otherwise(0) + \
            when(
                col('relocs') == True, 1
            ).otherwise(0) + \
            when(
                col('nx') == True, 1
            ).otherwise(0) + \
            when(
                col('canary') == True, 1
            ).otherwise(0)
        ) / 4
    ).select(
        'file_hash',
        'ratio'
    ).join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum(
            'ratio'
        ).alias('bin_info_ratio_total'),
        count(
            'file_hash'
        ).alias('bin_info_binary_count')
    )


def prepare_code_security_ratios(in_dataframe):

    # yapf: disable
    out_dataframe = in_dataframe.withColumn(
        'relro_full_percent',
        in_dataframe["relro_full_count"] / in_dataframe["total_bin_info_info_count"]
    ).withColumn(
        'aslr_percent',
        in_dataframe["aslr"] / in_dataframe["total_bin_info_info_count"]
    ).withColumn(
        'dep_percent',
        in_dataframe["dep"] / in_dataframe["total_bin_info_info_count"]
    ).withColumn(
        'stackguard_percent',
        in_dataframe["stackguard"] / in_dataframe["total_bin_info_info_count"]
    ).select(
        'firmware_hash',
        'relro_full_percent',
        'dep_percent',
        'aslr_percent',
        'stackguard_percent',
        'total_bin_info_info_count',
        'count_with_security_features'
    ).withColumnRenamed(
        'total_bin_info_info_count',
        'binaries_in_firmware'
    )
    # yapf: enable
    return out_dataframe


# yapf: disable
# The code security score consists of a percent rank over the average of the respective ratios of
# files for which a particular safety feature is enabled with respect to the other firmwares in the
# current dataset, be it all of those in the entire set or those within the same partition.
# Since this is a score of 'badness', we take 1 - avg(ratios).
def prepare_code_security_counts(code_security_ratios_df: DataFrame) -> DataFrame:
    ratio_score = 1 - ((code_security_ratios_df["relro_full_percent"] + code_security_ratios_df["aslr_percent"] + code_security_ratios_df["dep_percent"] + code_security_ratios_df["stackguard_percent"]) / 4)

    cs_with_score_df = code_security_ratios_df.withColumn(
        'code_security_score',
        ratio_score
    ).select(
        'firmware_hash',
        'binaries_in_firmware',
        'code_security_score'
    )

    return cs_with_score_df
# yapf: enable


def prepare_vxworks_data(file_tree_dataframe: DataFrame, vxworks_safety_features_dataframe: DataFrame) -> DataFrame:
    # Upcoming
    #
    # interrupt_stack_protection will need to consider whether or not the overflow / underflow
    # values are set to something non-zero in order for the protection to be valid
    # Currently that metadata doesn't exist
    #
    # user_stack_protection will need to check whether the overflow / underflow values
    # are non-zero, as well as the global_stack fill. For now, that metadata is either shaky
    # or does not exist.
    #
    # kernel_stack_protection will need to be considered. Currently this metadata
    # doesn't really exist, so the output will be rough.
    #
    # in write_protection, the vector table protection is _not_ a cross-platform feature,
    # and as such, computing statistics on it would be complex; we're going to avoid that for now
    vxworks_features_counted = vxworks_safety_features_dataframe.select(
        col('file_hash'),
        when(
            col('password_protection') == True, 1
        ).otherwise(0).alias('password_protection_count'),
        when(
            col('interrupt_stack_protection.guard_zones') == True, 1
        ).otherwise(0).alias('interrupt_stack_protection_count'),
        # If they have at least one of these two, we're going to give
        # them good boy points.
        when(
            (col('write_protection.user_text') == False) & \
            (col('write_protection.kernel_text') == False), 0
            # col('write_protection.virtual_mem_text') == True, 1
        ).otherwise(1).alias('write_protection_count_preliminary'),
        col('write_protection.virtual_mem_text').alias('virtual_mem_text'),
        when(
            col('kernel_stack_protection.guard_overflow_size_exec').isNotNull() & \
            col('kernel_stack_protection.guard_underflow_size_exec').isNotNull() & \
            col('kernel_stack_protection.guard_overflow_size_exception').isNotNull(), 1
        ).otherwise(None).alias('kernel_stack_protection_count'),
        when(
            (col('user_task_stack_protection.no_exec') == True) & \
            (col('user_task_stack_protection.guard_zones') == True), 1
        ).otherwise(0).alias('user_task_stack_protection_count'),
        when(
            col('file_hash').isNotNull(), 1
        ).otherwise(0).alias('row_count')
    ).withColumn(
        # Nested filter: If user text protection and kernel text protection are
        # disabled but virtual mem text is enabled, then this facet is secure
        # Otherwise, we should call it insecure
        'write_protection_count',
        when(
            (col('write_protection_count_preliminary') == 0) & \
            (col('virtual_mem_text') == True), 1
        ).otherwise(
            when(
                col('write_protection_count_preliminary') == 1, 1
            ).otherwise(0)
        )
    ).select(
        'file_hash',
        'row_count',
        'write_protection_count',
        'password_protection_count',
        'kernel_stack_protection_count',
        'interrupt_stack_protection_count',
        'user_task_stack_protection_count',
    ).withColumn(
        'has_one_security_feature',
        when(
            (col('write_protection_count') != 0) | \
            (col('password_protection_count') != 0) | \
            (col('kernel_stack_protection_count') != 0) | \
            (col('interrupt_stack_protection_count') != 0) | \
            (col('user_task_stack_protection_count') != 0), 1
        ).otherwise(0)
    )

    vxworks_safety_stats_df = vxworks_features_counted.join(
        file_tree_dataframe,
        'file_hash'
    ).groupby(
        'firmware_hash'
    ).agg(
        _sum('row_count').cast('int').alias('total_vxworks_count'),
        _sum('write_protection_count').cast('int').alias('write_protection'),
        _sum('password_protection_count').cast('int').alias('password_protection'),
        _sum('has_one_security_feature').cast('int').alias('count_with_security_features'),
        _sum('kernel_stack_protection_count').cast('int').alias('kernel_stack_protection'),
        _sum('interrupt_stack_protection_count').cast('int').alias('interrupt_stack_protection'),
        _sum('user_task_stack_protection_count').cast('int').alias('user_task_stack_protection'),
    ).select(
        'firmware_hash',
        'write_protection',
        'password_protection',
        'total_vxworks_count',
        'kernel_stack_protection',
        'interrupt_stack_protection',
        'user_task_stack_protection',
        'count_with_security_features'
    )
    return vxworks_safety_stats_df


def prepare_vxworks_features_per_binary(file_tree_dataframe: DataFrame, vxworks_safety_features_dataframe: DataFrame) -> DataFrame:
    return vxworks_safety_features_dataframe.select(
        col('file_hash'),
        when(
            col('password_protection') == True, 1
        ).otherwise(0).alias('password_protection_count'),
        when(
            col('interrupt_stack_protection.guard_zones') == True, 1
        ).otherwise(0).alias('interrupt_stack_protection_count'),
        # If they have at least one of these two, we're going to give
        # them good boy points.
        when(
            (col('write_protection.user_text') == False) & \
            (col('write_protection.kernel_text') == False), 0
            # col('write_protection.virtual_mem_text') == True, 1
        ).otherwise(1).alias('write_protection_count_preliminary'),
        col('write_protection.virtual_mem_text').alias('virtual_mem_text'),
        when(
            col('kernel_stack_protection.guard_overflow_size_exec').isNotNull() & \
            col('kernel_stack_protection.guard_underflow_size_exec').isNotNull() & \
            col('kernel_stack_protection.guard_overflow_size_exception').isNotNull(), 1
        ).otherwise(None).alias('kernel_stack_protection_count'),
        when(
            (col('user_task_stack_protection.no_exec') == True) & \
            (col('user_task_stack_protection.guard_zones') == True), 1
        ).otherwise(0).alias('user_task_stack_protection_count')
    ).withColumn(
        # Nested filter: If user text protection and kernel text protection are
        # disabled but virtual mem text is enabled, then this facet is secure
        # Otherwise, we should call it insecure
        'write_protection_count',
        when(
            (col('write_protection_count_preliminary') == 0) & \
            (col('virtual_mem_text') == True), 1
        ).otherwise(
            when(
                col('write_protection_count_preliminary') == 1, 1
            ).otherwise(0)
        )
    ).withColumn(
        'ratio',
        (
            col('write_protection_count') + \
            col('password_protection_count') + \
            when(
                col('kernel_stack_protection_count').isNotNull(), 1
            ).otherwise(0) + \
            col('interrupt_stack_protection_count') + \
            col('user_task_stack_protection_count')
        ) / 5
    ).select(
        'file_hash',
        'ratio'
    ).join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum(
            'ratio'
        ).alias(
            'vxworks_ratio_total'
        ),
        count(
            'file_hash'
        ).alias('vxworks_binary_count')
    )


def prepare_vxworks_security_ratios(vxworks_safety_stats_dataframe: DataFrame) -> DataFrame:
    ratios_dataframe = vxworks_safety_stats_dataframe.withColumn(
        'write_protection_percent',
        col('write_protection') / col('total_vxworks_count')
    ).withColumn(
        'password_protection_percent',
        col('password_protection') / col('total_vxworks_count')
    ).withColumn(
        'kernel_stack_protection_percent',
        col('kernel_stack_protection') / col('total_vxworks_count')
    ).withColumn(
        'interrupt_stack_protection_percent',
        col('interrupt_stack_protection') / col('total_vxworks_count')
    ).withColumn(
        'user_task_stack_protection_percent',
        col('user_task_stack_protection') / col('total_vxworks_count')
    ).select(
        'firmware_hash',
        'write_protection_percent',
        'password_protection_percent',
        'kernel_stack_protection_percent',
        'interrupt_stack_protection_percent',
        'user_task_stack_protection_percent',
        'count_with_security_features',
        col('total_vxworks_count').alias('vxworks_binary_count')
    )

    return ratios_dataframe


def prepare_vxworks_security_counts(vxworks_security_ratios_df: DataFrame) -> DataFrame:
    # Arithmetic mean on ratios
    # FIXME: Needs vxworks_security_ratios_df['kernel_stack_protection_percent'] + \
    # once that data is hardened / finalized
    ratio_score = 1 - (
        (
            vxworks_security_ratios_df['write_protection_percent'] + \
            vxworks_security_ratios_df['password_protection_percent'] + \
            vxworks_security_ratios_df['interrupt_stack_protection_percent'] + \
            vxworks_security_ratios_df['user_task_stack_protection_percent']
        ) / 4
    )
    vxs_with_score_df = vxworks_security_ratios_df.withColumn(
        'vxworks_security_score',
        ratio_score
    ).select(
        'firmware_hash',
        'vxworks_binary_count',
        'vxworks_security_score'
    )
    return vxs_with_score_df


def prepare_binary_safety_scores(file_tree_dataframe: DataFrame, bin_info_info_dataframe: DataFrame, vxworks_safety_features_dataframe: DataFrame):
    bin_info_features_per_binary = prepare_binary_info_features_per_binary(file_tree_dataframe, bin_info_info_dataframe)
    vxworks_features_per_binary = prepare_vxworks_features_per_binary(file_tree_dataframe, vxworks_safety_features_dataframe)

    scored_firmware_dataframe = bin_info_features_per_binary.join(
        vxworks_features_per_binary,
        'firmware_hash',
        'full'
    ).na.fill(0).withColumn(
        'score',
        (col('bin_info_ratio_total') + col('vxworks_ratio_total')) / (col('bin_info_binary_count') + col('vxworks_binary_count'))
    ).select(
        'firmware_hash',
        'score'
    )
    return scored_firmware_dataframe

# ////\\\\//////\\\\///////\\///\///
# ////    Crypto Materials   ///\\\/
# ////\\\\//////\\\\///////\\///\///
# The crypto materials score is composed of four facets:
# Each the number of SSH RSA Public and Private keys,
# the count of 'authorized keys files' found in the firmware,
# and the count of 'host keys' found in the firmware.
# Once these counts are found, the resultant sum is then used in a
# percent rank against all of the other firmwares, or at least those
# in the same partition as the firmware of interest.
def prepare_crypto_materials_counts(crypto_material_stats_df: DataFrame) -> DataFrame:
    crypto_total = crypto_material_stats_df["authorized_keys_count"] + crypto_material_stats_df["ssh_rsa_private_key_count"] + crypto_material_stats_df["host_keys_count"] + crypto_material_stats_df["pgp_private_key_count"] + crypto_material_stats_df["pkcs8_private_key_count"] + crypto_material_stats_df["pkcs12_certificate_count"] + crypto_material_stats_df["ssl_private_key_count"]
    crypto_material_with_score_df = crypto_material_stats_df.withColumn('crypto_total', crypto_total)

    return crypto_material_with_score_df


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash, file hash and file_full_path
# crypto_material_dataframe: a dataframe representing crypto material, down-selected to file hash and material type
# Output
# crypto_materials_stats_df: a dataframe containing counts of host keys, authorized keys, private keys, public keys, and the firmware hash
def prepare_crypto_material_data(file_tree_dataframe: DataFrame, crypto_material_dataframe: DataFrame) -> DataFrame:
    # yapf: disable
    vulnerable_crypto_materials_stats_dataframe = crypto_material_dataframe.select(
        when(
            col('material_type') == 'SshRsaPrivateKeyBlock', 1
        ).otherwise(0).alias('has_ssh_rsa_private_key'),
        when(
            col('material_type') == 'SshRsaPublicKeyBlock', 1
        ).otherwise(0).alias('has_ssh_rsa_public_key'),
        when(
            col('material_type') == 'PgpPrivateKeyBlock', 1
        ).otherwise(0).alias('has_pgp_private_key'),
        when(
            col('material_type') == 'Pkcs8PrivateKey', 1
        ).otherwise(0).alias('has_pkcs8_private_key'),
        when(
            col('material_type') == 'Pkcs12Certificate', 1
        ).otherwise(0).alias('has_pkcs12_certificate'),
        when(
            col('material_type') == 'SSLPrivateKey', 1
        ).otherwise(0).alias('has_ssl_private_key'),
        'file_hash'
    )



    vulnerable_crypto_materials_counts = vulnerable_crypto_materials_stats_dataframe.join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('has_ssh_rsa_private_key').cast('int').alias('ssh_rsa_private_key_count'),
        _sum('has_ssh_rsa_public_key').cast('int').alias('ssh_rsa_public_key_count'),
        _sum('has_pgp_private_key').cast('int').alias('pgp_private_key_count'),
        _sum('has_pkcs8_private_key').cast('int').alias('pkcs8_private_key_count'),
        _sum('has_pkcs12_certificate').cast('int').alias('pkcs12_certificate_count'),
        _sum('has_ssl_private_key').cast('int').alias('ssl_private_key_count'),
    ).select(
        'firmware_hash',
        'ssh_rsa_private_key_count',
        'ssh_rsa_public_key_count',
        'pgp_private_key_count',
        'pkcs8_private_key_count',
        'pkcs12_certificate_count',
        'ssl_private_key_count'
    )

    # Checking col('file_full_path').contains('ssh_host') before the full regex lets Spark filter most of the
    # rows out without having to uncompress the data from Tungsten to apply the regex, which results in a significant
    # speedup.

    host_and_authorized_key_counts = file_tree_dataframe.select(
        'file_hash',
        'firmware_hash',
        'file_full_path'
    ).distinct().select(
        'firmware_hash',
        when(
            (col('file_full_path').contains('ssh_host') & col('file_full_path').rlike('.*ssh_host.*key')), 1
        ).otherwise(0).alias('has_host_key'),
        when(
            col('file_full_path').endswith('authorized_keys') | col('file_full_path').endswith('authorized_keys2'), 1
        ).otherwise(0).alias('has_authorized_key'),
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('has_host_key').cast('int').alias('host_keys_count'),
        _sum('has_authorized_key').cast('int').alias('authorized_keys_count')
    ).select(
        'firmware_hash',
        'host_keys_count',
        'authorized_keys_count'
    )

    crypto_materials_stats_df = vulnerable_crypto_materials_counts.join(
        host_and_authorized_key_counts,
        'firmware_hash'
    )
    # yapf: enable
    return crypto_materials_stats_df


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# cyclomatic_complexity_dataframe: a dataframe representing cyclomatic complexity, without downselection
# Output
# a dataframe with a firmware hash and the max complexity found amongst all of the binaries for that firmware
def prepare_cyclomatic_complexity_data(file_tree_dataframe: DataFrame, cyclomatic_complexity_dataframe: DataFrame) -> DataFrame:
    # yapf: disable
    cyclomatic_complexity_max_dataframe = cyclomatic_complexity_dataframe.groupBy(
        'file_hash'
    ).agg(
        _max('complexity').alias('max_complexity_in_file')
    )

    cyclomatic_complexity_stats_df = cyclomatic_complexity_max_dataframe.join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _max('max_complexity_in_file').alias('max_complexity')
    )
    # yapf: enable
    return cyclomatic_complexity_stats_df


# ////\\\\//////\\\\///////\\///\///
# ////  Memory Corruptions   ///\\\/
# ////\\\\//////\\\\///////\\///\///
def prepare_memory_corruptions_counts(mc_stats_df: DataFrame) -> DataFrame:
    # yapf: disable
    mc_with_score_df = mc_stats_df.withColumn(
        'memory_corruptions_per_binary', mc_stats_df["memory_corruption_count"] / mc_stats_df["memory_corruption_binary_count"]
    ).select(
        'firmware_hash',
        'memory_corruptions_per_binary'
    )
    # yapf: enable
    return mc_with_score_df


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# memory_corruptions_dataframe: a dataframe representing memory corruptions, down-selected to file hash
# Output
# a dataframe containing a firmware hash, the number of memory corruptions across all files, not counted distinctly,
# and the number of memory corruptions across distinctly counted files.
def prepare_memory_corruptions_data(file_tree_dataframe: DataFrame, memory_corruptions_dataframe: DataFrame) -> DataFrame:
    # yapf: disable
    memory_corruptions_statistics_df = file_tree_dataframe.join(
        memory_corruptions_dataframe,
        'file_hash'
    ).select(
        'file_hash',
        'firmware_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        countDistinct('file_hash').cast('int').alias('memory_corruption_binary_count'),
        count('file_hash').cast('int').alias('memory_corruption_count')
    )
    # yapf: enable
    return memory_corruptions_statistics_df


# ////\\\\//////\\\\///////\\///\///
# ////       Code Analysis   ///\\\/
# ////\\\\//////\\\\///////\\///\///
# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# code_analysis_python_dataframe: a dataframe representing python code analysis, not downselected.
# Output
# A dataframe with counts of each combination of severity and confidence for issues found in code.
# yapf: disable
def prepare_code_analysis_data(file_tree_dataframe: DataFrame, code_analysis_python_dataframe: DataFrame) -> DataFrame:
    return code_analysis_python_dataframe.groupBy(
        'file_hash'
    ).agg(
        _sum(
            when(
                (col('issue_severity') == 'HIGH') & (col('issue_confidence') == 'HIGH'), 1
            ).otherwise(0)
        ).alias('hs_hc'),
        _sum(
            when(
                (col('issue_severity') == 'HIGH') & (col('issue_confidence') == 'MEDIUM'), 1
            ).otherwise(0)
        ).alias('hs_mc'),
        _sum(
            when(
                (col('issue_severity') == 'HIGH') & (col('issue_confidence') == 'LOW'), 1
            ).otherwise(0)
        ).alias('hs_lc'),
        _sum(
            when(
                (col('issue_severity') == 'MEDIUM') & (col('issue_confidence') == 'HIGH'), 1
            ).otherwise(0)
        ).alias('ms_hc'),
        _sum(
            when(
                (col('issue_severity') == 'MEDIUM') & (col('issue_confidence') == 'MEDIUM'), 1
            ).otherwise(0)
        ).alias('ms_mc'),
        _sum(
            when(
                (col('issue_severity') == 'MEDIUM') & (col('issue_confidence') == 'LOW'), 1
            ).otherwise(0)
        ).alias('ms_lc'),
        _sum(
            when(
                (col('issue_severity') == 'LOW') & (col('issue_confidence') == 'HIGH'), 1
            ).otherwise(0)
        ).alias('ls_hc'),
        _sum(
            when(
                (col('issue_severity') == 'LOW') & (col('issue_confidence') == 'MEDIUM'), 1
            ).otherwise(0)
        ).alias('ls_mc'),
        _sum(
            when(
                (col('issue_severity') == 'LOW') & (col('issue_confidence') == 'LOW'), 1
            ).otherwise(0)
        ).alias('ls_lc'),
    ).withColumn(
        'weighted_file_risk',
        (col('hs_hc') * 10) +
        (col('hs_mc') * 5) +
        (col('hs_lc') * 2) +
        (col('ms_hc') * 5) +
        (col('ms_mc') * 2.5) +
        (col('ms_lc') * 1) +
        (col('ls_hc') * 2) +
        (col('ls_mc') * 1) +
        (col('ls_lc') * 0.4)
    ).join(
        file_tree_dataframe,
        'file_hash'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum(
            'weighted_file_risk'
        ).cast('float').alias('total_weighted_file_risk'),
        _sum(
            'hs_hc'
        ).cast('int').alias('high_severity_high_confidence'),
        _sum(
            'hs_mc'
        ).cast('int').alias('high_severity_medium_confidence'),
        _sum(
            'hs_lc'
        ).cast('int').alias('high_severity_low_confidence'),
        _sum(
            'ms_hc'
        ).cast('int').alias('medium_severity_high_confidence'),
        _sum(
            'ms_mc'
        ).cast('int').alias('medium_severity_medium_confidence'),
        _sum(
            'ms_lc'
        ).cast('int').alias('medium_severity_low_confidence'),
        _sum(
            'ls_hc'
        ).cast('int').alias('low_severity_high_confidence'),
        _sum(
            'ls_mc'
        ).cast('int').alias('low_severity_medium_confidence'),
        _sum(
            'ls_lc'
        ).cast('int').alias('low_severity_low_confidence')
    )


# ////\\\\//////\\\\///////\\///\///
# ////      Credentials      ///\\\/
# ////\\\\//////\\\\///////\\///\///

# The credentials score is simply a percent rank over the sum of the credentials
# found in the /etc/passwd and /etc/shadow files, with respect to the other firmwares
# in the current dataset, be it all of those in the entire set or those within the same
# partition. Currently, we do not distinguish between occasions on which passwords were
# not found in these files and occasions on which we found no such files at all. There
# is room for improvement in this score due to that: the concept of 'safety' with regard
# to passwords is made more difficult because we are unable to make such distinctions.
# For example, if Firmware A has such files yet no hashes were discovered, it would be
# given the same score as Firmware B for which no files were discovered, and yet the
# same score as Firmware C for which unpack failed entirely.
def prepare_credentials_counts(creds_stats_df: DataFrame) -> DataFrame:
    creds_with_score_df = creds_stats_df.withColumn(
        'total_creds', creds_stats_df["creds_from_passwd"] + creds_stats_df["creds_from_shadow"]
    ).select(
        'firmware_hash',
        'total_creds'
    )

    return creds_with_score_df


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# passwd_dataframe: a dataframe representing passwd_parse, down-selected to user, password and file hash
# shadow_dataframe: a dataframe representing shadow_parse, down-selected to user, password and file hash
# Output
# A dataframe containing firmware hash and counts of credentials found in each passwd and shadow
def prepare_creds_data(file_tree_dataframe: DataFrame, passwd_dataframe: DataFrame, shadow_dataframe: DataFrame) -> DataFrame:
    # yapf: disable
    exclusion_list = ['x', '*', '!', '!!', '0', '\\', '1000', '/fabos', '']

    passwd_statistics_df = file_tree_dataframe.join(
        passwd_dataframe,
        'file_hash'
    ).select(
        'firmware_hash',
        'user',
        'password'
    ).distinct().where(
        ~col('password').isin(*exclusion_list)
    ).groupBy(
        'firmware_hash'
    ).agg(
        count('password').cast('int').alias('creds_from_passwd')
    ).select(
        'firmware_hash',
        'creds_from_passwd'
    )

    shadow_statistics_df = file_tree_dataframe.join(
        shadow_dataframe,
        'file_hash'
    ).select(
        'firmware_hash',
        'user',
        'password'
    ).distinct().where(
        ~col('password').isin(*exclusion_list)
    ).groupBy(
        'firmware_hash'
    ).agg(
        count('password').cast('int').alias('creds_from_shadow')
    ).select(
        'firmware_hash',
        'creds_from_shadow'
    )

    all_creds_statistics_df = passwd_statistics_df.join(
        shadow_statistics_df,
        'firmware_hash',
        'full'
    ).select(
        'creds_from_shadow',
        'creds_from_passwd',
        'firmware_hash'
    ).na.fill(0)

    return all_creds_statistics_df
    # yapf: enable


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# version_multiplicity_dataframe: a dataframe representing software components, down-selected to name, version and file hash
# Output
# A dataframe containing firmware hash and software version multiplicity
def prepare_version_multiplicity_data(sbom_dataframe: DataFrame) -> DataFrame:
    vm_with_score_df = sbom_dataframe.select(
        'firmware_hash',
        explode_outer(
            'components'
        ).alias(
            'component'
        )
    ).select(
        'firmware_hash',
        col('component.name').alias('name'),
        col('component.version').alias('version'),
    ).na.drop(
        subset=['version']
    ).groupBy(
        'firmware_hash',
        'name'
    ).agg(
        countDistinct('version').alias('version_count')
    ).select(
        'firmware_hash',
        when(
            col('version_count') > 1, col('version_count') - 1
        ).otherwise(0).alias('multiplicity')
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('multiplicity').cast('int').alias('software_version_multiplicity')
    )

    return vm_with_score_df


# ////\\\\//////\\\\///////\\///\///
# ////    Function Safety    ///\\\/
# ////\\\\//////\\\\///////\\///\///
def prepare_function_safety_counts(fs_stats_df: DataFrame) -> DataFrame:
    # yapf: disable
    fs_with_score_df = fs_stats_df.withColumn(
        'unsafe_percent', fs_stats_df["unsafe_count"] / (fs_stats_df["safe_count"] + fs_stats_df["unsafe_count"])
    ).select(
        'firmware_hash',
        'unsafe_percent'
    )
    return fs_with_score_df


# Arguments
# file_tree_dataframe: a dataframe representing a file tree, down-selected to at-minimum firmware hash and file hash
# function_info_dataframe: a dataframe representing function info, down-selected to file hash and func name
# function_safety_dataframe: a dataframe representing function safety, not down-selected
# Output
# A dataframe containing the count of safe function calls and unsafe function calls, and a firmware hash
def prepare_function_safety_data(file_tree_dataframe: DataFrame, function_info_dataframe: DataFrame, function_safety_dataframe: DataFrame) -> DataFrame:
    # yapf: disable
    securities_on_files = function_info_dataframe.join(
        function_safety_dataframe,
        _lower(function_info_dataframe['func_name']) == _lower(function_safety_dataframe['name'])
    ).select(
        'file_hash',
        when(
            col('type') == 'safe', 1
        ).otherwise(0).alias('safe_count'),
        when(
            col('type') == 'unsafe', 1
        ).otherwise(0).alias('unsafe_count')
    ).groupBy(
        'file_hash'
    ).agg(
        _sum('safe_count').alias('safe_count'),
        _sum('unsafe_count').alias('unsafe_count')
    )

    securities_on_firmwares = file_tree_dataframe.join(
        securities_on_files,
        'file_hash'
    ).select(
        'firmware_hash',
        'safe_count',
        'unsafe_count'
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('safe_count').cast('int').alias('safe_count'),
        _sum('unsafe_count').cast('int').alias('unsafe_count')
    )
    # yapf: enable
    return securities_on_firmwares


# ////\\\\//////\\\\///////\\///\///
# ////     Firmware CVEs     ///\\\/
# ////\\\\//////\\\\///////\\///\///


# The firmware cve interval score is counted thus:
# We compare the current firmware's counts of each low, medium, high and critical
# CVEs to those of all other firmwares, or if a partition column is specified, against
# all of those firmwares in the same partition. This comparison is a percent rank for
# relativity.
# Once this has been calculated, we take a weighted mean of these percent rank scores
# such that the more dangerous CVE scores are counted a higher number of times. These
# weights can be easily changed as desired.
# Finally, the 'composite score' is ranked against all other firmwares, or at least those
# within the same partition.
def prepare_firmware_cve_counts(firmware_cves_df: DataFrame, firmware_hashes_df: DataFrame) -> DataFrame:
    # yapf: disable
    # Ensure that the windows for each of low, med, hi, and crit are over the entire firmware space instead of just
    # those for which a CVE is known to exist
    firmware_cves_full_df = firmware_hashes_df.join(
        firmware_cves_df,
        'firmware_hash',
        'left'
    ).na.fill(0)

    low_window = Window.orderBy(firmware_cves_full_df['low'])
    med_window = Window.orderBy(firmware_cves_full_df['medium'])
    high_window = Window.orderBy(firmware_cves_full_df['high'])
    crit_window = Window.orderBy(firmware_cves_full_df['critical'])
    # sum(wi*xi)/sum(wi)
    cve_composite_score = (percent_rank().over(low_window) + (2 * percent_rank().over(med_window)) + (3 * percent_rank().over(high_window)) + (4 * percent_rank().over(crit_window))) / 10

    fwc_with_score_df = firmware_cves_full_df.withColumn(
        'firmware_cve_component_score', cve_composite_score
    ).select(
        'firmware_hash',
        'firmware_cve_component_score'
    )
    # yapf: enable
    return fwc_with_score_df


# Arguments
# validated_vulnerabilities_dataframe: a dataframe representing validated vulnerabilities, down-selected to firmware hash, cve id and severity
# Output
# a dataframe containing a firmware hash and counts of cves binned into groups by severity of the cve
def prepare_vulnerability_data(validated_vulnerabilities_dataframe: DataFrame) -> DataFrame:
    # yapf: disable

    final_dataframe = validated_vulnerabilities_dataframe.select(
        'firmware_hash',
        when(
            col('severity') < 4, 1
        ).otherwise(0).alias('low'),
        when(
            (col('severity') >= 4) & (col('severity') < 7), 1
        ).otherwise(0).alias('medium'),
        when(
            (col('severity') >= 7) & (col('severity') < 9), 1
        ).otherwise(0).alias('high'),
        when(
            col('severity') >= 9, 1
        ).otherwise(0).alias('critical')
    ).groupBy(
        'firmware_hash'
    ).agg(
        _sum('low').cast('int').alias('low'),
        _sum('medium').cast('int').alias('medium'),
        _sum('high').cast('int').alias('high'),
        _sum('critical').cast('int').alias('critical')
    )
    # yapf: enable
    return final_dataframe

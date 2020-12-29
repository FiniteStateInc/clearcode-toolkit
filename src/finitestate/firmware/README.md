# Common Firmware Library

Behold, you have found the Finite State common library for all things firmware.

## Contents

- fsglue
- schemas
- s3

## fsglue

The `fsglue` folder contains libraries for interacting with AWS Glue, as well as some Spark utilities.

#### glue.py functions & descriptions

- `load_dataframe_from_glue_table(database: str, table_name: str, glue_context: awsglue.context.GlueContext) -> pyspark.sql.DataFrame`: a utility for loading data from glue tables into dataframes.

- `downselect_dataframe(dataframe: pyspark.sql.DataFrame, list_of_columns_to_select: list) -> pyspark.sql.DataFrame`: a utility for selecting desired columns out of a dataframe by providing a list. _This should be deprecated_.

- `publish_jsonl_to_s3(key: str, row: pyspark.sql.Row, target_bucket: str, max_retries=5: int, validate_payload_checksum=False: boolean) -> dict`: a utility for saving individual rows to S3. Returns a write status dictionary.

- `publish_custom_cloudwatch_glue_metric(cloudwatch_client: boto3.client, job_name: str, job_run_ids: list, metric_name: str, value: int, unit=None: str, namespace=None: str) -> None`: This publishes custom cloudwatch metrics to cloudwatch. It is often used in glue jobs to get statistics on write counts. On failure, raises.

- `publish_df_as_jsonl(df: pyspark.sql.DataFrame, get_key_for_row: str, target_bucket: str) -> int`: publishes a dataframe to S3 in the form of JSONL documents. This function returns a writecount, or 0 on failure.

- `read_firmware_file_tree(glue_database: str, fw_sha256: str) -> pyspark.sql.DataFrame`: This utility will read the jsonl file tree for a single firmware into a dataframe.

- `read_firmware_analytics_from_tree(glue_database: str, table_name: str, file_tree_df: pyspark.sql.DataFrame, schema: pyspark.sql.types.StructType) -> pyspark.sql.DataFrame`: This utility will read the jsonl analysis results for a single firmware into a dataframe.

#### stats.py overview

This is the library where statistical pyspark transformation functions are held.

The following are used to generate approximate risk scores for firmwares:

- `get_statistic_from_dataframe(count_dataframe: pyspark.sql.DataFrame, statistic_name: str) -> Union[int, float]`: Given a dataframe containing one row, this function attempts to extract a number from its `statistic_name` column.

- `calculate_quantiles(dataframe: DataFrame, column: str, tolerance: float) -> dict`: This function takes a dataframe and attempts to calculate quantiles on a particular column of that dataframe, from 1-100. The resultant document is shaped as follows, where ai are the approximated quantiles:

```
{
    1: a1,
    ...
    99: a99
}
```

- `interpolate_quantile(statistic: int, quantile_document: dict) -> float`: using the statistic from `get_statistic_from_dataframe` and the quantile document from `calculate_quantiles` ( loaded from s3), this will place that statistic amongst the quantiles as an approximate risk score. If the statistic is greater than the largest quantile value of the document, a score of 0.99 is returned. If the statistic is 0, a score of 0.0 is returned.

#### General Overview for preparation of counts and statistics

You will notice several functions named like `prepare_w+_counts` and `prepare_w+_data`. The purpose of the _data_ preparation functions should be to get the raw analysis data into countable formats -- this is, these functions make quantitative dataframes from qualitative ones. The purpose of the _count_ preparation function should be to take a numerical dataframe(s) and return a dataframe containing a statistic that meaningfully describes the underlying analysis data.

For example, let's look at `prepare_creds_data` and `prepare_credentials_counts`.

`prepare_creds_data(file_tree_dataframe: DataFrame, passwd_dataframe: DataFrame, shadow_dataframe: DataFrame) -> DataFrame` will take analysis results from the `passwd_parse` and `shadow_parse` plugins, returning a dataframe that outlines the count of hardcoded credentials from `shadow` and `passwd` documents in a particular firmware. In particular, a row resulting from this function has `firmware_hash`, `creds_from_shadow`, and `creds_from_passwd` as its columns. This data is interesting and useful on its own, and as such is made queryable by one of our glue jobs. During risk calculation, this data is fed through `prepare_credentials_counts`:

`prepare_credentials_counts(creds_stats_df: DataFrame) -> DataFrame` will take a credentials statistics dataframe which is produced as a result of feeding `passwd_parse` and `shadow_parse` results through `prepare_creds_data`, and produce a new dataframe with `firmware_hash` and `total_creds` as columns. After this dataframe is produced, firmwares will be compared to each other by percent ranks, approximate quantiles, or other means by using the `total_creds` statistic from this dataframe.

All of the 'preparation' type functions in the stats library follow this general format, and since they are transformation functions, should always take only dataframes as arguments, and yield dataframes as arguments. This makes unit testing simpler.

## schemas

The `schemas` folder contains schemas for various datasets in our firmware domain. Schemas are formed using PySpark's typing API. Schemas are typically used when loading data using Spark when Spark's ability to infer a schema based on found data is in doubt. As a simple example schema, take a look at the `credentials_statistics` schema:

```
import pyspark.sql.types

credentials_statistics_parquet_schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField('firmware_hash', pyspark.sql.types.StringType()),
    pyspark.sql.types.StructField('creds_from_shadow', pyspark.sql.types.IntegerType()),
    pyspark.sql.types.StructField('creds_from_passwd', pyspark.sql.types.IntegerType()),
])
```

Tools for generation of schemas and test data can be found in `finitestate/scripts/testing`. The `generate_schema_from_serverless` script can be used _only when a table is defined in a serverless document_.

Importing schemas is very simple. Import the `get_schema` function ( found in `__init__` ) and use it:

```
from finitestate.firmware.schemas import get_schema
credentials_statistics_schema = get_schema('credentials_statistics')

```

## S3

This contains functions for interacting with S3 that didn't seem to fit elsewhere. They should probably be deprecated at some point.

- `write_approximated_risk_score(bucket: str, firmware_hash: str, score_type: str, score: float)`: For a particular firmware hash, this will write an approximate risk score document to S3 as JSON. It's shaped as follows:

```
{
    "firmware_hash": string,
    "score_type": score (float)
}
```

- `load_approximate_quantile_document_from_s3(bucket: str, quantile_type: str) -> dict:` This function will load the approximate quantile document from S3 for the given quantile type. The following quantile types are currently available:

```
code_analysis
code_security
config_management
credentials
crypto_material
cves
cyclomatic_complexity
function_safety
memory_corruptions
```

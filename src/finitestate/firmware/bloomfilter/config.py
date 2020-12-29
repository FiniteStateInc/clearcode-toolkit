# Sets an error rate of %0.025 (1 in 4,000) with a capacity of 5MM items.
# See https://hur.st/bloomfilter/?n=5000000&p=0.00025&m=& for more information
# 5MM was chosen for being a whole number roughly 2x the size of our most dense sparse plugin output in late July 2020.

DEFAULT_ERROR_RATE = 0.00025
DEFAULT_CAPACITY = 5000000
KEY_PREFIX = "fwan_dataset_bf:"

# The bloom filters are only useful for per-file-hash Firmware Analysis plugins with sparse output, as those are the
# scenarios where we are likely to avoid unnecessary object storage fetches.

exclusions = [
    "augeas",             # firmware-file-level
    "file_hashes",        # too dense
    "file_tree",          # firmware-level
    "file_type",          # too dense
    "unpack_report",      # irrelevant
    "unpack_failed",      # irrelevant
    "printable_strings",  # too dense
]

exclusion_prefixes = [
    "firmware_cpes",      # firmware-level
    "sbom",               # sbom/unified is firmware-level, sbom/apt_file is irrelevant
    "similarity_hash",    # irrelevant
]

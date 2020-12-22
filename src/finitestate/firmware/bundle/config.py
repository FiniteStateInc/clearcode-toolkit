from finitestate.firmware.datasets import Dataset

__all__ = [
    'is_dataset_excluded_from_bundles'
]

__EXCLUDED_DATASET_NAMES = [
    'base64_decoder',  # not useful for test runner
    'binary_analysis/ghidra_database',  # not useful for test runner
    'file_type',  # excluded due to being duplicative with file_tree
    'sbom/apt_file',  # unnecessary due to the unified sbom
    'software_components',  # unnecessary due to the unified sbom
    'unpack_failed',  # not useful for test runner
    'unpack_report',  # not useful for the test runner
    'firmware_cpes/file_cpes',  # firmware_cpes/sbom_cpes output already including the information
    'firmware_cpes/product_cpes',  # firmware_cpes/sbom_cpes output already includes this information
]

__EXCLUDED_DATASET_PREFIXES = [
    'library_match',  # not useful for the test runner
    'similarity_hash'  # not useful for the test runner
]


def is_dataset_excluded_from_bundles(dataset: Dataset) -> bool:
    """
    Indicates if a Dataset should be excluded from bundling for various reasons (mostly, that the dataset isn't useful
    to the Test Runner and/or the data is already captured in another form by a different Dataset).

    :param dataset: The Dataset to check for bundle exclusion
    """
    if dataset and dataset.name:
        return dataset.name in __EXCLUDED_DATASET_NAMES or any([dataset.name.startswith(prefix) for prefix in __EXCLUDED_DATASET_PREFIXES])


from typing import Optional

from .config import KEY_PREFIX, exclusions, exclusion_prefixes


def get_bloom_filter_key(dataset_name: Optional[str]) -> Optional[str]:
    if dataset_name:
        dataset_name = dataset_name.strip('/')
        if dataset_name not in exclusions:
            if not any([dataset_name.startswith(prefix) for prefix in exclusion_prefixes]):
                return f'{KEY_PREFIX}{dataset_name}'

from typing import Optional


def without_nones(d: Optional[dict]) -> dict:
    result = {}

    if d:
        for k, v in d.items():
            if isinstance(v, dict):
                v = without_nones(v)
            if v is not None:
                result[k] = v

    return result

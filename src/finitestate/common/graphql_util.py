import base64
import logging
from typing import Generator


logger = logging.getLogger(__name__)


def graphql_nodes(doc: dict, element_name: str) -> Generator[dict, None, None]:
    if doc:
        for edge in doc.get(element_name, {}).get('edges', []):
            node = edge.get('node')
            if node:
                yield node


def opaque_decode(value: str) -> str:
    return base64.urlsafe_b64decode(value.encode() + b"===").decode()


def opaque_decode_uuid(value: str, separator=':') -> str:
    try:
        decoded_str = opaque_decode(value)
        split_uuid = decoded_str.split(separator)
        return split_uuid[len(split_uuid) - 1]
    except Exception as e:
        logger.warning('GraphQL type name not found as opaque ID prefix', e)
        return decoded_str

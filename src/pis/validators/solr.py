"""Validators for CSV files."""

import requests
from loguru import logger
from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle
from otter.util.errors import TaskValidationError


def counts(src: str, data_type: str, dst: str, config: Config) -> None:
    """Check if the document counts at the remote and local locations match.

    :param src: The URL of the Solr instance.
    :type src: str
    :param data_type: The data type to query.
    :type data_type: str
    :param dst: The path to the destination CSV file.
    :type dst: str
    """
    params = {'q': '*:*', 'fq': f'type:{data_type}', 'rows': 0}
    response = requests.get(src, params=params, timeout=10)
    response.raise_for_status()
    src_count = response.json()['response']['numFound']

    h = StorageHandle(dst, config)
    f = h.open('r')
    dst_count = sum(1 for _ in f)
    dst_count -= 1  # subtract 1 for the header row

    logger.debug(f'source count: {src_count}, destination count: {dst_count}')
    if src_count != dst_count:
        raise TaskValidationError(f'mismatch in {data_type}: src {src_count} dst {dst_count}')

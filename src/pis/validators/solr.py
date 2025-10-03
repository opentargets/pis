"""Validators for CSV files."""

import subprocess
from pathlib import Path

import requests
from loguru import logger


# fastest way to count lines is calling wc -l
def _wccount(filename) -> int:
    out = subprocess.Popen(
        ['/usr/bin/wc', '-l', filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ).communicate()[0]
    return int(out.partition(b' ')[0])


def counts(url: str, data_type: str, local_path: Path) -> bool:
    """Check if the document counts at the remote and local locations match.

    :param url: The URL of the Solr instance.
    :type url: str
    :param data_type: The data type to query.
    :type data_type: str
    :param local_path: The path to the local CSV file.
    :type local_path: Path
    :return: True if the counts match, False otherwise.
    :rtype: bool
    """
    params = {'q': '*:*', 'fq': f'type:{data_type}', 'rows': 0}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    remote_count = response.json()['response']['numFound']
    local_count = _wccount(local_path) - 1  # subtract header
    logger.warning(f'remote count: {remote_count}, local count: {local_count}')
    return remote_count == local_count

"""Validators for Elasticsearch."""

import subprocess
from pathlib import Path

from elasticsearch import Elasticsearch as Es
from loguru import logger


# fastest way to count lines is calling wc -l
def _wccount(filename) -> int:
    out = subprocess.Popen(
        ['/usr/bin/wc', '-l', filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ).communicate()[0]
    return int(out.partition(b' ')[0])


def counts(url: str, index: str, local_path: Path) -> bool:
    """Check if the document counts at the remote and local locations match.

    :param url: The URL of the ElasticSearch instance.
    :type url: str
    :param index: The index to scan.
    :type index: str
    :param local_path: The path where the documents are stored locally.
    :type local_path: Path

    :return: True if the document counts match, False otherwise.
    :rtype: bool
    """
    logger.debug(f'checking if document counts at {url}/{index} and {local_path} match')

    es = Es(url)
    remote_doc_count = es.count(index=index)['count']
    local_doc_count = _wccount(local_path)

    logger.debug(f'checking if {remote_doc_count} == {local_doc_count}')
    return remote_doc_count == local_doc_count

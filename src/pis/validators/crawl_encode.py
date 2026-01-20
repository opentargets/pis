from pathlib import Path

from google.cloud import storage
from loguru import logger


def all_blobs_exist(paths: list[str]) -> bool:
    """Check if all files in the given list of paths exist.

    :param paths: A list of file paths to check.
    :type paths: list[str]

    :return: True if all files exist, False otherwise.
    :rtype: bool
    """
    client = storage.Client()
    for path in paths:
        bucket_name, blob_name = path.replace('gs://', '').split('/', 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logger.debug(f'checking if file exists: {path}')
        if not blob.exists():
            logger.error(f'File does not exist: {path}')
            return False

    return True


def all_files_exist(files: list[str]) -> bool:
    """Check if all files in the given list exist in the specified local directory.

    :param files: A list of file names to check for existence.
    :type files: list[str]

    :return: True if all files exist, False otherwise.
    :rtype: bool
    """
    for file in files:
        file_path = Path(file)
        logger.debug(f'checking if file exists: {file_path}')
        if not file_path.exists():
            logger.error(f'File does not exist: {file_path}')
            return False

    return True

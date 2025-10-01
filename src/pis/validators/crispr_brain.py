from pathlib import Path

from loguru import logger


def at_least_one_file_exist(path: Path, glob: str) -> bool:
    """Check if at least one file exists in the given directory matching the glob pattern.

    :param path: The directory path to check for files.
    :type path: Path
    :param glob: The glob pattern to match files (e.g., '*.csv.gz').
    :type glob: Path

    :return: True if at least one file exists, False otherwise.
    :rtype: bool
    """
    logger.debug(f'checking if at least one file exists like {glob} in {path}')

    return len(list(path.glob(glob))) > 0

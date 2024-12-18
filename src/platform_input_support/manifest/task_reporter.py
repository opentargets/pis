"""TaskReporter class and report decorator for logging and updating tasks in the manifest."""

import sys
from datetime import UTC, datetime
from functools import wraps

from loguru import logger

from platform_input_support.config import settings
from platform_input_support.manifest.models import Resource, Result, TaskManifest
from platform_input_support.util.errors import TaskAbortedError


class TaskReporter:
    """Class for logging and updating tasks in the manifest."""

    def __init__(self, name: str):
        self.name = name
        self._manifest: TaskManifest
        self._resources: list[Resource] = []

    def staged(self, log: str):
        """Set the task result to STAGED."""
        self._manifest.result = Result.STAGED
        self._manifest.staged = datetime.now(UTC)
        self._manifest.elapsed = (self._manifest.staged - self._manifest.created).total_seconds()
        logger.success(f'task staged: ran for {self._manifest.elapsed:.2f}s')

    def validated(self, log: str):
        """Set the task result to VALIDATED."""
        self._manifest.result = Result.VALIDATED
        logger.success(f'task validated: {log}')

    def completed(self, log: str):
        """Set the task result to COMPLETED."""
        self._manifest.result = Result.COMPLETED
        logger.success(f'task completed: {log}')

    def failed(self, error: Exception, where: str):
        """Set the task result to FAILED."""
        self._manifest.result = Result.FAILED
        logger.opt(exception=sys.exc_info()).error(f'task failed {where}: {error}')

    def aborted(self):
        """Set the task result to ABORTED."""
        self._manifest.result = Result.ABORTED
        logger.warning('task aborted')


def report(func):
    """Decorator for logging and updating tasks in the manifest."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            if func.__name__ == 'run':
                logger.info('task run started')
                # add task definition to the manifest
                self._manifest = self._manifest.model_copy(update={'definition': self.definition.__dict__}, deep=True)
            elif func.__name__ == 'validate':
                logger.info('task validation started')
            elif func.__name__ == 'upload':
                logger.info('task upload started')

            result = func(self, *args, **kwargs)

            if func.__name__ == 'run':
                self.staged(result.name)
            elif func.__name__ == 'validate':
                if settings().remote_uri:
                    self.validated(result.name)
                else:
                    self._resources.append(result.resource)
                    self.completed(result.name)

            elif func.__name__ == 'upload':
                self._resources.append(result.resource)
                self.completed(result.name)
            return result
        except Exception as e:
            kwargs['abort'].set()
            if isinstance(e, TaskAbortedError):
                self.aborted()
            else:
                self.failed(e, func.__name__)

    return wrapper

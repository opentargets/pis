"""Exports Google Spreadsheet's page as CSV."""

from typing import Self

from google.auth import default
from google.auth.transport.requests import AuthorizedSession
from loguru import logger
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError


class SpreadsheetDownloadError(OtterError):
    """Base class for Spreadsheet Download errors."""


class SpreadsheetDownloadSpec(Spec):
    """Configuration fields for the spreadsheet download task.

    This task has the following custom configuration fields:
        - sheet_id (str): The ID for the Spreadsheet document.
        - gid (str): The ID for the page to be exported.
        - destination (str): The path, relative to `release_uri` to upload the
            results to.
    """

    sheet_id: str
    gid: str
    destination: str


class SpreadsheetDownload(Task):
    """Download spreadsheet from Google Sheets.

    .. note:: `destination` will be prepended with the
        :py:obj:`otter.config.model.Config.release_uri` config field.

    If no `release_uri` is provided in the configuration, the results will only be
    stored locally. This is useful for local runs or debugging. The local path will
    be created by prepeding :py:obj:`otter.config.model.Config.work_path` to the
    destination field.
    """

    def __init__(self, spec: SpreadsheetDownloadSpec, context: TaskContext) -> None:
        super().__init__(spec, context)

    @report
    async def run(self) -> Self:
        logger.debug(f'exporting to csv gid: {self.spec.gid} from spreadsheet {self.spec.sheet_id}')

        scopes = [
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly',
        ]

        try:
            creds, project = default(scopes=scopes)
            logger.debug(f'Using default credentials from environment (project: {project})')
        except Exception as e:
            logger.error(f'Failed to load default credentials: {e}')
            raise SpreadsheetDownloadError(f'Failed to authenticate: {e}')

        session = AuthorizedSession(creds)

        logger.debug('exporting file')
        export_url = (
            f'https://docs.google.com/spreadsheets/d/{self.spec.sheet_id}/export?format=csv&gid={self.spec.gid}'
        )

        response = session.get(export_url)

        if not response.ok:
            error_msg = (
                f'error {response.status_code} getting the spreadsheet reason: '
                f'{response.reason}. Response body: {response.text}'
            )
            logger.error(error_msg)
            raise SpreadsheetDownloadError(error_msg)

        d = StorageHandle(self.spec.destination, self.context.config)
        dst = d.open('wb')

        dst.write(response.content)

        logger.debug('file exported')

        return self

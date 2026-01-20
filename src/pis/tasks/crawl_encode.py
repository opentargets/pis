"""Generate tasks for ENCODE file download."""

import asyncio
import csv  # use csv to skip additional dependencies.
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientError
from gcloud.aio.storage import Storage
from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.tasks.copy import CopySpec
from otter.validators import v
from pydantic import BaseModel, ValidationError, field_validator
from tqdm.asyncio import tqdm

from pis.validators.crawl_encode import all_blobs_exist, all_files_exist


class HTTPAsyncDownloadError(Exception):
    """Raised when there is an error downloading a file via HTTP asynchronously."""


class GCSUploadError(Exception):
    """Raised when there is an error uploading to GCS."""


class EncodeManifestMissingValueError(Exception):
    """Raised when a required value is missing from the ENCODE manifest file."""


class EncodeManifestSchemaValidationError(Exception):
    """Raised when the ENCODE manifest schema is invalid."""


class EncodeManifestSchema(BaseModel):
    """Schema for the ENCODE manifest file columns.

    The manifest is prepared via the ENCODE /report.tsv endpoint.

    In order to download the experiment files from the ENCODE portal, we need to:

    1) perform a `GET` request to the `/report.tsv` endpoint with the appropriate
       query parameters to generate the manifest file with searched experiments file URLs,
    2) parse the manifest file to extract the file URLs
    3) download the files using the pre-built endpoints

    - Example endpoint to download experiment bed.gz file:
        https://www.encodeproject.org/files/ENCFF002CTW/@@download/ENCFF002CTW.bed.gz
    - Example File URL in manifest: `/files/ENCFF002CTW/@@download/ENCFF002CTW.bed.gz`
    - Example Accession in manifest: `ENCFF002CTW`

    See https://app.swaggerhub.com/apis-docs/encodeproject/api/ for more details.
    """

    url_stem: str
    """Name that reflects the column with the relative path to the file to download."""
    accession: str
    """Name that reflects the column with the accession ID of the file to download."""

    def parse_manifest(self, manifest_path: Path) -> dict[str, str]:
        """Parse and validate the ENCODE manifest file.

        The manifest file is expected to be a TSV file with a header row.
        The first line of the file is expected to be a comment line
        with the search parameters used to generate the manifest.

        :param manifest_path: Path to the manifest file.
        :type manifest_path: Path
        :return: A dictionary mapping accession IDs to file URL stems.
        :rtype: dict[str, str]
        :raises EncodeManifestSchemaValidationError: If the manifest file is malformed.
        :raises EncodeManifestMissingValueError: If required values are missing in the manifest.

        """
        with open(manifest_path, encoding='utf-8') as f:
            search_parameters = f.readline()
            logger.info(f'ENCODE manifest search parameters: {search_parameters.strip()}')
            reader = csv.DictReader(f, delimiter='\t')

            # Perform the validation of the file
            if not reader.fieldnames:
                raise EncodeManifestSchemaValidationError('Manifest file is empty or malformed.')
            cols = (self.url_stem, self.accession)

            missing_cols = [c for c in cols if c not in reader.fieldnames]
            if missing_cols:
                logger.error(f'Expected to found columns in the manifest: {missing_cols}.')
                raise EncodeManifestSchemaValidationError(f'Manifest file is missing required column: {missing_cols}.')

            # Initial pass to check for missing values in required columns
            file_stems = {}
            for row in reader:
                url_stem = row.get(self.url_stem)
                accession = row.get(self.accession)
                if not url_stem or not accession:
                    msg = f'Missing required value in manifest row: url_stem="{url_stem}", accession="{accession}".'
                    logger.error(msg)
                    raise EncodeManifestMissingValueError(msg)
                file_stems[accession] = url_stem

        return file_stems


class CrawlEncodeTemplateVariable(StrEnum):
    """Template variables for CrawlEncode tasks."""

    URL_STEM = 'url_stem'
    ACCESSION = 'accession'


class CrawlEncodeSpec(Spec):
    """Configuration fields for the task for downloading ENCODE data files."""

    manifest: str
    """Source manifest file path.
        This file should be downloaded from the ENCODE portal using the `/report.tsv` endpoint."""
    columns: EncodeManifestSchema
    """Mapping of required columns in the manifest file."""
    expand: CopySpec
    """Copy task spec to perform for each file to download."""

    def model_post_init(self, __context: Any) -> None:
        """Allow for missing `accession` and `url_stem` keys in the global scratchpad.

        These keys will be populated dynamically based on the manifest column values
        to scratchpad during task execution, so each `COPY` subtask will have access to them for templating.
        """
        self.scratchpad_ignore_missing = True

    @field_validator('expand', mode='after')
    @classmethod
    def _check_patterns(cls, expand: CopySpec) -> CopySpec:
        """Ensure that the `expand` - CopySpec contains the required template variables.

        The `source` field must contain the `${url_stem}` template variable,
        and the `destination` field must contain the `${accession}` template variable.

        :param expand: CopySpec to validate.
        :type expand: CopySpec
        :return: Validated CopySpec.
        :rtype: CopySpec
        :raises ValidationError: If required template variables are missing.
        """
        if '${' + CrawlEncodeTemplateVariable.URL_STEM.value + '}' not in expand.source:
            msg = f'COPY task `source` must contain the `{CrawlEncodeTemplateVariable.URL_STEM.value}` template.'
            logger.error(msg)
            raise ValidationError(msg)

        if '${' + CrawlEncodeTemplateVariable.ACCESSION.value + '}' not in expand.destination:
            msg = f'COPY task `destination` must contain the `{CrawlEncodeTemplateVariable.ACCESSION.value}` template.'
            logger.error(msg)
            raise ValidationError(msg)
        return expand

    def _resolve_destination(self, context: TaskContext) -> CopySpec:
        """Resolve the destination path for the download.

        :param context: TaskContext with global configuration.
        :type context: TaskContext
        :return: CopySpec with resolved destination path.
        :rtype: CopySpec
        :raises GCSUploadError: If the `release_uri` is invalid.

        Depending on whether `release_uri` is set in the global configuration,
        the destination path will be adjusted to point to either a local path (relative to `work_path`)
        or a GCS path (relative to the `release_uri`).
        """
        release_uri = context.config.release_uri
        if not release_uri:
            logger.info('Transferring files to local path')
            # Overwrite the destination to be local path
            full_path = context.config.work_path / Path(self.expand.destination)
            # Ensure the relative path exists
            full_path.parent.mkdir(parents=True, exist_ok=True)
            self.expand.destination = full_path.as_posix()

        else:
            logger.info(f'Transferring files to GCS path: {release_uri}')
            if not release_uri.startswith('gs://'):
                msg = f'release_uri must be a GCS path (gs://...), got: {release_uri}'
                logger.error(msg)
                raise GCSUploadError(msg)
            else:
                # Overwrite the destination to be GCS path
                dest_path = Path(self.expand.destination)
                # Sanitize the destination path to remove leading slashes
                if dest_path.is_absolute():
                    dest_path = dest_path.relative_to('/')
                self.expand.destination = f'{release_uri}/{dest_path.as_posix()}'
        return self.expand


class CrawlEncode(Task):
    """Download files from ENCODE based on a manifest file.

    The task performs `COPY task` for the index of files specified in the `manifest` file.
      The process is as follows:

      1) reads the manifest file and extracts the `stem_url` and `accession` columns,
      2) for each `stem_url` in the manifest, the relevant COPY task spec template is templated with the
            extracted `stem_url` and `accession` values.
      3) the templated COPY tasks are enqueued to the subtask queue for processing.
    """

    def __init__(self, spec: CrawlEncodeSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CrawlEncodeSpec
        self.manifest_local_path = context.config.work_path / Path(spec.manifest)
        self.scratchpad = Scratchpad({
            CrawlEncodeTemplateVariable.ACCESSION.value: '',
            CrawlEncodeTemplateVariable.URL_STEM.value: '',
        })
        self.transfer_specs = []

    @staticmethod
    async def _transfer_all(specs_list: list[CopySpec], max_concurrent: int) -> None:
        """Fetch all files specified in the list of CopySpec asynchronously.

        :param specs_list: List of CopySpec to process.
        :type specs_list: list[CopySpec]
        :param max_concurrent: Maximum number of concurrent downloads.
        :type max_concurrent: int
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        async with aiohttp.ClientSession() as session:
            tasks = [CrawlEncode._transfer(spec.source, spec.destination, session, semaphore) for spec in specs_list]
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Transferring files.'):
                await f

    @staticmethod
    async def _write_local_file(destination: str, response: aiohttp.ClientResponse) -> None:
        """Write content to a local file.

        :param destination: Local path to write to.
        :type destination: str
        :param response: aiohttp ClientResponse with the content to write.
        :type response: aiohttp.ClientResponse
        :raises IOError: If there is an error writing to the local file.
        """
        async with aiofiles.open(destination, 'wb') as fp:
            async for chunk in response.content.iter_chunked(1024 * 1024):
                await fp.write(chunk)

    @staticmethod
    async def _write_gcs_file(
        destination: str, response: aiohttp.ClientResponse, session: aiohttp.ClientSession
    ) -> None:
        """Write content to GCS file.

        This method consumes the response content and uploads it to GCS.

        :param destination: GCS path to write to (gs://bucket/path).
        :type destination: str
        :param response: aiohttp ClientResponse with the content to upload.
        :type response: aiohttp.ClientResponse
        :param session: aiohttp ClientSession to use for requests.
        :type session: aiohttp.ClientSession
        :raises GCSUploadError: If there is an error uploading to GCS.
        """
        client = Storage(session=session)
        bucket_name = destination.split('/')[2]
        blob_name = '/'.join(destination.split('/')[3:])
        logger.info(f'Uploading to GCS {destination}.')
        try:
            await client.upload(
                bucket=bucket_name,
                object_name=blob_name,
                file_data=await response.read(),
                content_type=response.headers.get('Content-Type'),
                session=session,
            )
        except Exception as e:
            logger.error(f'Failed to upload to GCS {destination}: {e}')
            raise GCSUploadError(f'Failed to upload to GCS {destination}.') from e

    @staticmethod
    async def _transfer(
        source: str,
        destination: str,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        retries: int = 3,
    ) -> None:
        """Asynchronously copy a file from source to destination using aiohttp.

        :param source: Source URL to download from.
        :type source: str
        :param destination: Destination path to write to (local path or gs:// path).
        :type destination: str
        :param session: aiohttp ClientSession to use for requests.
        :type session: aiohttp.ClientSession
        :param semaphore: Semaphore to limit concurrent downloads.
        :type semaphore: asyncio.Semaphore
        :param retries: Number of retries for failed downloads.
        :type retries: int
        """
        async with semaphore:
            for attempt in range(retries):
                try:
                    async with session.get(source, timeout=aiohttp.ClientTimeout(total=600)) as response:
                        response.raise_for_status()
                        if destination.startswith('gs://'):
                            await CrawlEncode._write_gcs_file(destination, response, session)
                        else:
                            await CrawlEncode._write_local_file(destination, response)
                    return
                except ClientError as e:
                    logger.warning(f'Attempt {attempt + 1} failed to fetch {source}: {e}')
                    if attempt == retries - 1:
                        logger.error(f'All {retries} attempts failed for {source}.')
                        raise HTTPAsyncDownloadError(f'Failed to download {source} after {retries} attempts.') from e
                    await asyncio.sleep(1 + attempt * 2)  # Exponential backoff

    @report
    def run(self) -> Self:
        """Run the ENCODE file download task.

        :return: Self instance.
        :rtype: Self
        """
        logger.info(f'Parsing ENCODE manifest file from {self.spec.manifest}.')
        file_stems = self.spec.columns.parse_manifest(self.manifest_local_path)
        logger.info(f'Found {len(file_stems)} files to download.')
        # Loop over each file stem
        resolved_template_spec = self.spec._resolve_destination(self.context)
        for accession, url_stem in file_stems.items():
            logger.info(f'Building task for experiment: {accession} from {url_stem} file.')
            self.scratchpad.store('accession', accession)
            self.scratchpad.store('url_stem', url_stem)

            # Substitute values in CopySpec and enqueue new tasks
            # Create a new CopySpec with the current scratchpad values
            subtask_spec = resolved_template_spec.model_validate(
                self.scratchpad.replace_dict(resolved_template_spec.model_dump())
            )
            self.transfer_specs.append(subtask_spec)
        asyncio.run(self._transfer_all(self.transfer_specs, max_concurrent=5))
        logger.info(f'Enqueued {len(self.transfer_specs)} download tasks from ENCODE manifest.')

        return self

    @report
    def validate(self) -> Self:
        """Check that the downloaded file exists and has a valid size.

        :return: Self instance.
        :rtype: Self
        """
        all_exist = all_blobs_exist if self.context.config.release_uri else all_files_exist
        paths = [spec.destination for spec in self.transfer_specs]
        v(all_exist, paths)
        return self

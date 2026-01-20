"""Generate tasks for ENCODE file download."""

import asyncio
import csv  # use csv to skip additional depenencies.
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientError
from google.cloud import storage
from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.tasks.copy import CopySpec
from pydantic import BaseModel, field_validator
from tqdm.asyncio import tqdm


class EncodeManifestMissingValuError(Exception):
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
        :return: A dictionary mapping accession IDs to file URL stems.
        :raises EncodeManifestSchemaValidationError: If the manifest file is malformed.
        :raises EncodeManifestMissingValuError: If required values are missing in the manifest.

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
                    raise EncodeManifestMissingValuError(msg)
                file_stems[accession] = url_stem

        return file_stems


class TemplateVariable(StrEnum):
    """Template variables for ENCODE manifest processing."""

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

    @field_validator('do', mode='after')
    @classmethod
    def _check_patterns(cls, expand: CopySpec) -> CopySpec:
        """Ensure that the `destination` field in the CopySpec contains required templated variables."""
        if '${' + TemplateVariable.URL_STEM.value + '}' in expand.destination:
            msg = f'COPY task `destination` must contain the `{TemplateVariable.URL_STEM.value}` templated variable.'
            logger.error(msg)
            raise ValueError(msg)
        return expand


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
            TemplateVariable.ACCESSION.value: '',
            TemplateVariable.URL_STEM.value: '',
        })

    @staticmethod
    async def _copy_all(specs_list: list[CopySpec], max_concurrent: int) -> None:
        """Fetch all files specified in the list of CopySpec asynchronously.

        :param specs_list: List of CopySpec to process.
        :param max_concurrent: Maximum number of concurrent downloads.
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        queue = asyncio.Queue()
        async with aiohttp.ClientSession() as session:
            tasks = [CrawlEncode._copy(spec.source, spec.destination, session, semaphore) for spec in specs_list]
            for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Transferring files.'):
                await f

    @staticmethod
    async def _copy(
        source: str, destination: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, retries: int = 3
    ) -> None:
        """Asynchronously copy a file from source to destination using aiohttp."""
        async with semaphore:
            for attempt in range(retries):
                try:
                    async with session.get(source, timeout=aiohttp.ClientTimeout(total=600)) as response:
                        response.raise_for_status()
                        async with aiofiles.open(destination, 'wb') as fp:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                await fp.write(chunk)
                    return  # Success, exit the function
                except ClientError as e:
                    logger.warning(f'Attempt {attempt + 1} failed to download {source}: {e}')
                    if attempt == retries - 1:
                        logger.error(f'All {retries} attempts failed for {source}.')
                        raise
                    await asyncio.sleep(1 + attempt * 2)  # Exponential backoff

    @report
    def run(self) -> Self:
        """Run the ENCODE file download task."""
        logger.info(f'Parsing ENCODE manifest file from {self.spec.manifest}.')
        file_stems = self.spec.columns.parse_manifest(self.manifest_local_path)
        logger.info(f'Found {len(file_stems)} files to download.')
        template_spec = self.spec.expand
        # Loop over each file stem
        specs = []
        for accession, url_stem in file_stems.items():
            logger.info(f'Building task for experiment: {accession} from {url_stem} file.')
            self.scratchpad.store('accession', accession)
            self.scratchpad.store('url_stem', url_stem)

            # Substitute values in CopySpec and enqueue new tasks
            # Create a new CopySpec with the current scratchpad values
            subtask_spec = template_spec.model_validate(self.scratchpad.replace_dict(template_spec.model_dump()))
            specs.append(subtask_spec)

        self._

        logger.info(f'Enqueued {len(specs)} download tasks from ENCODE manifest.')

        return self

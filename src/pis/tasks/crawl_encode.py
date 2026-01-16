"""Generate tasks for ENCODE file download."""

import csv  # use csv to skip additional depenencies.
from enum import StrEnum
from pathlib import Path
from queue import Queue
from typing import Any, Self

from loguru import logger
from otter.scratchpad.model import Scratchpad
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.tasks.copy import CopySpec
from pydantic import BaseModel


class TemplateVariable(StrEnum):
    """Template variables for ENCODE manifest processing."""

    URL_STEM = 'url_stem'
    ACCESSION = 'accession'


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

    @model_validator('do', mode='after')
    def _check_patterns(cls, expand: CopySpec) -> CopySpec:
        """Ensure that the `destination` field in the CopySpec contains required templated variables."""
        if "${" + TemplateVariable.URL_STEM. in expand.destination:
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
        self.scratchpad = Scratchpad({'accession': '', 'url_stem': ''})

    @report
    def run(self) -> Self:
        """Run the ENCODE file download task."""
        logger.info(f'Processing ENCODE manifest file from {self.spec.manifest}.')
        file_stems = self.spec.columns.parse_manifest(self.manifest_local_path)
        new_tasks = 0
        # Loop over each file stem
        for accession, url_stem in file_stems.items():
            subtask_queue: Queue[Spec] = self.context.sub_queue
            logger.info(f'Building task for experiment: {accession} from {url_stem} file.')
            self.scratchpad.store('accession', accession)
            self.scratchpad.store('url_stem', url_stem)

            # Substitute values in CopySpec and enqueue new tasks
            for do_spec in self.spec.do:
                # Create a new CopySpec with the current scratchpad values
                subtask_spec = do_spec.model_validate(self.scratchpad.replace_dict(do_spec.model_dump()))
                subtask_spec.task_queue = subtask_queue
                subtask_queue.put(subtask_spec)
                new_tasks += 1
        logger.info(f'Enqueued {new_tasks} download tasks from ENCODE manifest.')
        return self

"""Fetch data from the IMPC SOLR API and save as CSV files."""
import shutil
import tempfile
from pathlib import Path
from typing import Self

import requests
from loguru import logger
from otter.manifest.model import Artifact
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination
from retry import retry

# The tables and their fields to fetch from SOLR
IMPC_SOLR_TABLES = {
    # Mouse to human mappings.
    'gene_gene': ('gene_id', 'hgnc_gene_id'),
    'ontology_ontology': ('mp_id', 'hp_id'),
    # Mouse model and disease data.
    'mouse_model': ('model_id', 'model_phenotypes'),
    'disease': ('disease_id', 'disease_phenotypes'),
    'disease_model_summary': (
        'model_id',
        'model_genetic_background',
        'model_description',
        'disease_id',
        'disease_term',
        'disease_model_avg_norm',
        'disease_model_max_norm',
        'marker_id',
    ),
    'ontology': ('ontology', 'phenotype_id', 'phenotype_term'),
}


class ImpcSolrError(OtterError):
    """Base class for IMPC SOLR fetch errors."""


class ImpcSolrSpec(Spec):
    """Configuration for the IMPC SOLR fetch task.

    Custom fields:
        - destination (str): Directory to save the CSV files.
        - solr_host (str): SOLR API endpoint URL.
        - batch_size (int): Number of records to fetch per batch.
        - timeout (int): Request timeout in seconds.
        - data_types (list[str] | None): Specific data types to fetch. If None, fetches all.
    """

    destination: str
    solr_host: str = 'http://www.ebi.ac.uk/mi/impc/solr/phenodigm/select'
    batch_size: int = 1000000000  # Large batch to get all records at once
    timeout: int = 3600
    data_types: list[str] | None = None


class ImpcSolr(Task):
    """Fetch IMPC SOLR data and save as CSV files."""

    def __init__(self, spec: ImpcSolrSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ImpcSolrSpec

        # Resolve local destination
        self.local_dir: Path = context.config.work_path / spec.destination_dir

        # Resolve remote destination if configured
        self.remote_uri: str | None = None
        if context.config.release_uri:
            self.remote_uri = f'{context.config.release_uri}/{spec.destination_dir}'

        # Determine which data types to fetch
        self.data_types_to_fetch = spec.data_types or list(IMPC_SOLR_TABLES.keys())

        # Validate data types
        invalid_types = set(self.data_types_to_fetch) - set(IMPC_SOLR_TABLES.keys())
        if invalid_types:
            raise ImpcSolrError(f'Invalid data types: {invalid_types}. Valid types: {list(IMPC_SOLR_TABLES.keys())}')

    @retry(tries=3, delay=5, backoff=1.2, jitter=(1, 3))
    def _get_number_of_solr_records(self, data_type: str) -> int:
        """Get the total number of records for a given data type."""
        params = {'q': '*:*', 'fq': f'type:{data_type}', 'rows': 0}
        try:
            response = requests.get(self.spec.solr_host, params=params, timeout=self.spec.timeout)
            response.raise_for_status()
            return response.json()['response']['numFound']
        except requests.RequestException as e:
            raise ImpcSolrError(f'Error getting record count for {data_type}: {e}') from e

    @retry(tries=3, delay=5, backoff=1.2, jitter=(1, 3))
    def _query_solr_batch(self, data_type: str, start: int) -> tuple[int, str]:
        """Request one batch of SOLR records and return the count and temp file path."""
        columns = [col.split(' > ')[0] for col in IMPC_SOLR_TABLES[data_type]]
        params = {
            'q': '*:*',
            'fq': f'type:{data_type}',
            'start': start,
            'rows': self.spec.batch_size,
            'wt': 'csv',
            'fl': ','.join(columns),
        }
        try:
            response = requests.get(self.spec.solr_host, params=params, timeout=self.spec.timeout, stream=True)
            response.raise_for_status()
        except requests.RequestException as e:
            raise ImpcSolrError(f'Error querying SOLR for {data_type} starting at {start}: {e}') from e

        # Write records to temp file as they arrive to avoid keeping everything in memory
        with tempfile.NamedTemporaryFile('wt', delete=False) as tmp_file:
            response_lines = response.iter_lines(decode_unicode=True)
            header = next(response_lines)

            # Only write header for the first batch
            if start == 0:
                tmp_file.write(header + '\n')

            record_count = 0
            for line in response_lines:
                record_count += 1
                tmp_file.write(line + '\n')

            return record_count, tmp_file.name

    def _fetch_data_type(self, data_type: str, output_file: Path) -> None:
        """Fetch all rows of the requested data type to the specified file."""
        logger.info(f'Fetching data for {data_type}')

        total_records = self._get_number_of_solr_records(data_type)
        if total_records == 0:
            raise ImpcSolrError(f'SOLR returned no data for {data_type}')

        logger.info(f'Found {total_records} records for {data_type}')

        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'wb') as outfile:
            start, total = 0, 0

            while True:
                record_count, tmp_filename = self._query_solr_batch(data_type, start)

                # Copy temp file content to output file
                with open(tmp_filename, 'rb') as tmp_file:
                    shutil.copyfileobj(tmp_file, outfile)

                # Clean up temp file
                Path(tmp_filename).unlink()

                # Update counters
                start += self.spec.batch_size
                total += record_count

                logger.debug(f'Downloaded {total}/{total_records} records for {data_type}')

                # Exit when all records have been retrieved
                if total == total_records:
                    break

    @report
    def run(self) -> Self:
        """Run the IMPC SOLR data fetching task."""
        # Prepare destination directory
        check_destination(self.local_dir, delete=True)
        self.local_dir.mkdir(parents=True, exist_ok=True)

        # Fetch each data type
        output_files = []
        for data_type in self.data_types_to_fetch:
            output_file = self.local_dir / f'impc_solr_{data_type}.csv'
            self._fetch_data_type(data_type, output_file)
            output_files.append(output_file)

        logger.info(f'Successfully fetched {len(self.data_types_to_fetch)} data types')

        # Upload to remote storage if configured
        if self.remote_uri:
            remote_storage = get_remote_storage(self.remote_uri)
            for output_file in output_files:
                remote_path = f'{self.remote_uri}/{output_file.name}'
                remote_storage.upload(output_file, remote_path)

        # Set up artifacts
        self.artifacts = [
            Artifact(
                source=f'{self.spec.solr_host}?fq=type:{data_type}',
                destination=(
                    f'{self.remote_uri}/impc_solr_{data_type}.csv'
                    if self.remote_uri
                    else str(self.local_dir / f'impc_solr_{data_type}.csv')
                ),
            )
            for data_type in self.data_types_to_fetch
        ]

        return self

    @report
    def validate(self) -> Self:
        """Validate the fetched data files."""
        # Check that all expected files exist and are non-empty
        for data_type in self.data_types_to_fetch:
            output_file = self.local_dir / f'impc_solr_{data_type}.csv'

            if not output_file.exists():
                raise ImpcSolrError(f'Output file {output_file} does not exist')

            if output_file.stat().st_size == 0:
                raise ImpcSolrError(f'Output file {output_file} is empty')

            # Basic CSV validation - check that it has at least a header line
            try:
                with open(output_file) as f:
                    first_line = f.readline().strip()
                    if not first_line:
                        raise ImpcSolrError(f'Output file {output_file} appears to be empty')
            except OSError as e:
                raise ImpcSolrError(f'Error reading output file {output_file}: {e}') from e

        logger.info('All IMPC SOLR data files validated successfully')
        return self

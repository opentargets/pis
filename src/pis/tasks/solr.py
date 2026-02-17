"""Fetch data from the a Solr API and save as CSV files."""

from collections.abc import Generator
from typing import IO, Self

import requests
from loguru import logger
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from pis.validators.solr import counts


class SolrError(OtterError):
    """Base class for SOLR fetch errors."""


class SolrSpec(Spec):
    """Configuration for the SOLR fetch task.

    This task fetches data from the SOLR API and saves it as CSV files.

    Currently, it filters collections by a type, and it can optionally take a set
    of fields for each collection to filter the documents.

    Custom fields:
        - destination (str): Directory to save the CSV files.
        - url (str): Solr API endpoint URL, including the core and /select path.
        - batch_size (int): Number of records to fetch per batch.
        - data_type: str: The value of the `type` field by which to filter the
            SOLR collection.
        - fields: list[str]: Optional list of fields to include in the output.
            If not provided, all fields will be included.
    """

    destination: str
    url: str
    batch_size: int = 100000
    data_type: str
    fields: list[str] | None = None


class Solr(Task):
    """Fetch Solr data and save as CSV files."""

    def __init__(self, spec: SolrSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: SolrSpec
        self.session: requests.Session
        self.dst = f'{self.spec.destination}/{self.spec.data_type}.csv'

    def _count_docs(self, data_type: str) -> int:
        """Return the number of documents for a given data type."""
        params = {'q': '*:*', 'fq': f'type:{data_type}', 'rows': 0}
        response = self.session.get(self.spec.url, params=params)
        response.raise_for_status()
        count = response.json()['response']['numFound']
        logger.debug(f'found {count} records for {data_type}')
        return count

    def _fetch_docs(self) -> Generator[requests.Response]:
        """Yield documents for a given data type in batches."""
        start = 0
        batch_size = self.spec.batch_size
        count = self._count_docs(self.spec.data_type)
        params = {
            'q': '*:*',
            'fq': f'type:{self.spec.data_type}',
            'fl': ','.join(self.spec.fields) if self.spec.fields else None,
            'rows': batch_size,
            'wt': 'csv',
        }

        while start < count:
            response = self.session.get(self.spec.url, params={**params, 'start': start}, stream=True)
            response.raise_for_status()
            logger.trace(f'streaming {self.spec.data_type} rows {start}-{start + batch_size}')
            yield response
            start += batch_size

    def _save_docs(self, response: requests.Response, file: IO, trim_header: bool = True) -> None:
        """Stream response chunks directly to file."""
        total_bytes = 0
        for chunk in response.iter_content(chunk_size=32768):
            if chunk:
                if trim_header:
                    chunk = chunk[chunk.find(b'\n') + 1 :]
                file.write(chunk)
                total_bytes += len(chunk)
        logger.info(f'wrote {total_bytes / (1024**2):.2f}MiB to {self.dst}')

    @report
    def run(self) -> Self:
        """Run the Solr data fetching task."""
        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))

        h = StorageHandle(self.dst, self.context.config)
        dst = h.open('wb')

        for batch in self._fetch_docs():
            self._save_docs(batch, dst, trim_header=dst.tell() > 0)
        logger.info(f'fetched data type {self.spec.data_type} to {h.absolute}')

        return self

    @report
    def validate(self) -> Self:
        counts(self.spec.url, self.spec.data_type, self.dst, self.context.config)

        return self

"""Fetch data from the a Solr API and save as CSV files."""

from collections.abc import Generator
from pathlib import Path
from typing import IO, Any, Self

import requests
from loguru import logger
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination
from otter.validators import v
from otter.validators.file import file_exists
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


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
        - queries (list[dict[str, list[str]]]): List of queries to execute. Each
            query is a dict with a `data_type` str, and, optionally, a `fields`
            list. If `fields` is not provided, all fields for the data_type
            will be fetched.
    """

    destination: str
    url: str
    batch_size: int = 100000
    queries: list[dict[str, Any]]


class Solr(Task):
    """Fetch Solr data and save as CSV files."""

    def __init__(self, spec: SolrSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: SolrSpec
        self.session: requests.Session

        self.local_path: Path = context.config.work_path / spec.destination
        if context.config.release_uri:
            self.remote_uri = f'{context.config.release_uri}/{spec.destination}'

    def _count_docs(self, data_type: str) -> int:
        """Return the number of documents for a given data type."""
        params = {'q': '*:*', 'fq': f'type:{data_type}', 'rows': 0}
        response = self.session.get(self.spec.url, params=params)
        response.raise_for_status()
        count = response.json()['response']['numFound']
        logger.debug(f'found {count} records for {data_type}')
        return count

    def _fetch_docs(self, data_type: str, fields: list[str] | None) -> Generator[requests.Response]:
        """Yield documents for a given data type in batches."""
        start = 0
        batch_size = self.spec.batch_size
        count = self._count_docs(data_type)
        params = {
            'q': '*:*',
            'fq': f'type:{data_type}',
            'fl': ','.join(fields) if fields else None,
            'rows': batch_size,
            'wt': 'csv',
        }

        while start < count:
            response = self.session.get(self.spec.url, params={**params, 'start': start}, stream=True)
            response.raise_for_status()
            logger.trace(f'streaming {data_type} rows {start}-{start + batch_size}')
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
        logger.info(f'wrote {total_bytes / (1024**2):.2f}MiB to {file.name}')

    @report
    def run(self) -> Self:
        """Run the Solr data fetching task."""
        check_destination(self.local_path, delete=True)

        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1)))

        for q in self.spec.queries:
            data_type, fields = q['data_type'], q.get('fields')
            output_file = self.local_path / f'{data_type}.csv'
            check_destination(output_file, delete=True)

            with open(output_file, 'wb') as f:
                for batch in self._fetch_docs(data_type, fields):
                    self._save_docs(batch, f, trim_header=f.tell() > 0)
            logger.info(f'fetched data type {data_type} to {output_file}')

        return self

    @report
    def validate(self) -> Self:
        data_types = [
            {
                'data_type': self.spec.queries[i]['data_type'],
                'local_path': self.local_path / f'{self.spec.queries[i]["data_type"]}.csv',
            }
            for i in range(len(self.spec.queries))
        ]

        for dt in data_types:
            v(file_exists, dt['local_path'])
            # v(counts, self.spec.url, dt['data_type'], dt['local_path'])

        return self

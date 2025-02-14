"""Download fields from all documents in ElasticSearch indexes."""

import json
import sys
from pathlib import Path
from typing import Any, Self

import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch as Es
from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.helpers import ScanError
from loguru import logger
from otter.manifest.model import Artifact
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError, TaskAbortedError
from otter.util.fs import check_fs
from otter.validators import v

from pis.validators.elasticsearch import counts

BUFFER_SIZE = 20000


class ElasticsearchError(OtterError):
    """Base class for Elasticsearch errors."""


class ElasticsearchSpec(Spec):
    """Configuration fields for the elasticsearch task.

    This task has the following custom configuration fields:
        - url (str): The URL of the Elasticsearch instance.
        - destination (str): The path, relative to `release_uri` to upload the
            results to.
        - index (str): The index to scan.
        - fields (list[str]): The fields to retrieve from the documents
    """

    url: str
    destination: str
    index: str
    fields: list[str]


class Elasticsearch(Task):
    """Download fields from all documents in ElasticSearch indexes.

    .. note:: `destination` will be prepended with the :py:obj:`otter.config.model.Config.release_uri`
         config field.

    If no `release_uri` is provided in the configuration, the results will only be
    stored locally. This is useful for local runs or debugging. The local path will
    be created by prepeding :py:obj:`otter.config.model.Config.work_path` to the
    destination field.
    """

    def __init__(self, spec: ElasticsearchSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: ElasticsearchSpec
        self.es: Es
        self.doc_count: int = 0
        self.doc_written: int = 0
        self.local_path: Path = context.config.work_path / spec.destination
        self.remote_uri: str | None = None
        if context.config.release_uri:
            self.remote_uri = f'{context.config.release_uri}/{spec.destination}'
        self.destination = self.remote_uri or str(self.local_path)

    def _close_es(self) -> None:
        if hasattr(self, 'es'):
            self.es.close()
            del self.es

    def _write_docs(self, docs: list[dict[str, Any]], destination: Path) -> None:
        try:
            with open(destination, 'a+') as f:
                for d in docs:
                    json.dump(d, f)
                    f.write('\n')
            self.doc_written += len(docs)

        except OSError as e:
            self._close_es()
            raise ElasticsearchError(f'error writing to {destination}: {e}')

        logger.debug(f'wrote {len(docs)} ({self.doc_written}/{self.doc_count}) documents to {destination}')
        logger.debug(f'the dict was taking up {sys.getsizeof(docs)} bytes of memory')
        docs.clear()

    @report
    def run(self) -> Self:
        check_fs(self.local_path)

        logger.debug(f'connecting to elasticsearch at {self.spec.url}')
        try:
            self.es = Es(self.spec.url)
        except ElasticsearchException as e:
            self._close_es()
            raise ElasticsearchError(f'connection error: {e}')

        logger.debug(f'scanning index {self.spec.index} with fields {self.spec.fields}')
        try:
            self.doc_count = self.es.count(index=self.spec.index)['count']
        except ElasticsearchException as e:
            self._close_es()
            raise ElasticsearchError(f'error getting index count on index {self.spec.index}: {e}')
        logger.info(f'index {self.spec.index} has {self.doc_count} documents')

        buffer: list[dict[str, Any]] = []
        try:
            for hit in elasticsearch.helpers.scan(
                client=self.es,
                index=self.spec.index,
                query={'query': {'match_all': {}}, '_source': self.spec.fields},
            ):
                buffer.append(hit['_source'])
                if len(buffer) >= BUFFER_SIZE:
                    logger.trace('flushing buffer')
                    self._write_docs(buffer, self.local_path)
                    buffer.clear()

                    # we can use this moment to check for abort signals and bail out
                    if self.context.abort and self.context.abort.is_set():
                        raise TaskAbortedError
        except ScanError as e:
            logger.warning(f'error scanning index {self.spec.index}: {e}')
            raise ElasticsearchError(f'error scanning index {self.spec.index}: {e}')

        self._write_docs(buffer, self.local_path)
        self._close_es()
        logger.debug('scan complete')

        # upload the result to remote storage
        if self.remote_uri:
            remote_storage = get_remote_storage(self.remote_uri)
            remote_storage.upload(self.local_path, self.remote_uri)
        logger.debug('upload successful')

        self.artifacts = [Artifact(source=f'{self.spec.url}/{self.spec.index}', destination=str(self.destination))]
        return self

    @report
    def validate(self) -> Self:
        v(
            counts,
            self.spec.url,
            self.spec.index,
            self.local_path,
        )

        return self

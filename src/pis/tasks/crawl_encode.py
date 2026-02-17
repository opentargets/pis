"""Extract IDs from ENCODE TSV manifest."""

import csv
from typing import Self

from loguru import logger
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report


class CrawlEncodeSpec(Spec):
    """Configuration for the crawl_encode task."""

    source: str
    """Path (relative to the root of the release) to ENCODE's manifest file."""
    destination: str
    """Output path (relative to the root of the release)."""


class CrawlEncode(Task):
    """Extract IDs from ENCODE TSV manifest."""

    def __init__(self, spec: CrawlEncodeSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CrawlEncodeSpec

    @report
    def run(self) -> Self:
        logger.info(f'reading encode manifest from {self.spec.source}.')

        s, _ = StorageHandle(self.spec.source, config=self.context.config).read_text()
        reader = csv.reader(s.splitlines(), delimiter='\t')
        next(reader, None)  # skip header
        next(reader, None)

        sources = [f'https://www.encodeproject.org/files/{row[1]}/@@download/{row[1]}.bed.gz' for row in reader]

        d = StorageHandle(self.spec.destination, config=self.context.config)
        d.write_text('\n'.join(sources))

        logger.info(f'wrote {len(sources)} ids to {self.spec.destination}.')
        return self

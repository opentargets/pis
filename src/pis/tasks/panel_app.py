"""Fetch data from the PanelApp API and save as a JSONL file."""

import csv
import json
import time
from typing import IO, Self

from loguru import logger
from otter.manifest.model import Artifact
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report

MAX_RETRIES = 5


class PanelAppSpec(Spec):
    """Configuration for the PanelApp fetch task.

    Custom fields:
        - evidence_file (str): Path to the PanelApp evidence file.
        - api_url (str): URL for the PanelApp API endpoint.
        - destination (str): Path to save output file to.
    """

    evidence_file: str
    api_url: str
    destination: str


class PanelApp(Task):
    """Fetch data from the PanelApp API and save as a JSONL file."""

    def __init__(self, spec: PanelAppSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: PanelAppSpec

    def _fetch_and_save_panels(
        self,
        api_url: str,
        panel_id_list: list[str],
        dst: IO,
    ) -> list[str]:
        """Fetch and save panels from API given a list of panel ids."""
        panels = []
        sources = []

        for panel_id in panel_id_list:
            src = f'{api_url}/{panel_id}'
            sources.append(src)
            logger.debug(f'fetching data from {src}')
            s = StorageHandle(src, self.context.config)

            for attempt in range(MAX_RETRIES):
                try:
                    panel_data, _ = s.read_text()
                    panel_json = json.loads(panel_data.strip())
                    panels.append(json.dumps(panel_json))
                    break
                except Exception as e:
                    if '429 Client Error' in str(e) and attempt < MAX_RETRIES - 1:
                        retry_delay = 30 * (attempt + 1)
                        logger.warning(f'rate limited at {panel_id}, retrying in {retry_delay}s')
                        time.sleep(retry_delay)
                        continue
                    if '404 Client Error' in str(e):
                        logger.warning(f'panel {panel_id} not found at {src}, skipping')
                        break
                    logger.error(f'error fetching data for panel {panel_id} from {src}: {e}')
                    raise

        dst.write('\n'.join([str(panel) for panel in panels]))
        return sources

    @report
    def run(self) -> Self:
        """Run the PanelApp task."""
        h = StorageHandle(self.spec.evidence_file, self.context.config)
        with h.open('r') as f:
            panel_ids = {row['Panel Id'] for row in csv.DictReader(f, delimiter='\t')}
        all_panel_ids = list(panel_ids)
        logger.info(f'extracted {len(all_panel_ids)} panel ids from {self.spec.evidence_file}')

        d = StorageHandle(self.spec.destination, self.context.config)
        dst = d.open('w')

        sources = self._fetch_and_save_panels(self.spec.api_url, all_panel_ids, dst)
        self.artifacts = [Artifact(source=sources, destination=d.absolute)]
        logger.info(f'fetched and saved data to {d.absolute}')

        return self

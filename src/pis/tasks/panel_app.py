"""Fetch data from the PanelApp API and save as a JSONL file."""

import csv
import json
from pathlib import Path
from time import sleep
from typing import Self

import requests
from loguru import logger
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination


class PanelAppError(OtterError):
    """Base class for PanelApp fetch errors."""


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
        self.session: requests.Session

        self.evidence_file_local_path = context.config.work_path / spec.evidence_file
        self.destination_local_path: Path = context.config.work_path / spec.destination
        self.destination_remote_uri = None
        if context.config.release_uri:
            self.destination_remote_uri = f'{context.config.release_uri}/{spec.destination}'

    def _get_all_panel_ids(self, evidence_file: Path, panel_id_col_name: str) -> list[str]:
        """Get all panel ids from the evidence file."""
        with open(evidence_file) as f:
            panel_ids = {row[panel_id_col_name] for row in csv.DictReader(f, delimiter='\t')}
        return list(panel_ids)

    def _fetch_and_save_panels(
        self,
        session: requests.Session,
        api_url: str,
        panel_id_list: list[str],
        output_path: Path,
    ) -> None:
        """Fetch and save panels from API given a list of panel ids.

        If API rate limit prevents data fetching, a temporary delay is implemented until data is successfully fetched.
        """
        while len(panel_id_list) > 0:
            url = f'{api_url}/{panel_id_list[0]}'
            logger.debug(f'fetching data from {url}')
            try:
                panel_data = session.get(url, timeout=60).json()
                with open(output_path, 'a') as f:
                    f.write(json.dumps(panel_data) + '\n')
                panel_id_list.pop(0)
            except requests.JSONDecodeError:  # due to too many requests
                sleep(10)

    @report
    def run(self) -> Self:
        """Run the PanelApp task."""
        # Prepare destination
        check_destination(self.destination_local_path, delete=True)

        self.session = requests.Session()

        all_panel_ids = self._get_all_panel_ids(self.evidence_file_local_path, 'Panel Id')
        logger.info(f'extracted {len(all_panel_ids)} panel ids from {self.evidence_file_local_path}')

        self._fetch_and_save_panels(self.session, self.spec.api_url, all_panel_ids, self.destination_local_path)
        logger.info(f'fetched and saved data to {self.destination_local_path}')

        # Upload result if remote storage configured
        if self.destination_remote_uri:
            logger.debug(f'uploading file to {self.destination_remote_uri}')
            remote_storage = get_remote_storage(self.destination_remote_uri)
            remote_storage.upload(self.destination_local_path, self.destination_remote_uri)

        return self

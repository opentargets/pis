"""Fetch CRISPR Brain screens metadata and study result files.

This task downloads the CRISPR Brain screens JSON (gzipped response) and all
per-study hit lists (CSV.gz).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote

import requests
from loguru import logger
from otter.manifest.model import Artifact
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination
from otter.validators import v

from pis.validators.crispr_brain import at_least_one_file_exist


class CrisprBrainError(OtterError):
    """Base class for CRISPR Brain fetch errors."""


class CrisprBrainSpec(Spec):
    """Configuration for the CRISPR Brain fetch task.

    Custom fields:
        - destination (str): Path to save files to.
        - api_url (str): Full URL for the screens endpoint.
        - client_id (str): API client identifier.
        - study_prefix_url (str): Base URL for per-study CSV.GZ files.
        - validate_version (int | None): If provided, ensure API __version matches.
    """

    destination: str
    api_url: str
    client_id: str
    study_prefix_url: str
    validate_version: int | None


class CrisprBrain(Task):
    """Fetch CRISPR Brain screens metadata and per-study result files."""

    def __init__(self, spec: CrisprBrainSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CrisprBrainSpec

        # Resolve local destinations
        self.screens_local_path: Path = context.config.work_path / spec.destination / 'brain_screens.json.gz'
        self.studies_local_path = context.config.work_path / spec.destination / 'brain_studies'

        # Resolve remote destinations if release_uri is configured
        if context.config.release_uri:
            self.screens_remote_uri = f'{context.config.release_uri}/{spec.destination}/brain_screens.json.gz'
            self.studies_remote_uri = f'{context.config.release_uri}/{spec.destination}/brain_studies'

    def _post_screens(self) -> bytes:
        """POST to screens endpoint and return raw gzipped content."""
        response = requests.post(
            url=self.spec.api_url,
            data={'client_id': self.spec.client_id},
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=60,
        )
        response.raise_for_status()
        return response.content

    @staticmethod
    def _extract_study_ids_from_screens_gz(screens_gz: bytes) -> tuple[list[str], dict[str, Any]]:
        """Extract study identifiers and parsed JSON from gzipped screens payload."""
        json_text = gzip.decompress(screens_gz).decode()
        screens = json.loads(json_text)
        return [k for k in screens if not k.startswith('_')], screens

    def _download_study_csv_gz(self, session: requests.Session, study_id: str, destination: Path) -> None:
        logger.debug(f'downloading study {study_id} to {destination}')
        check_destination(destination, delete=True)
        response = session.get(f'{self.spec.study_prefix_url}{quote(study_id)}.csv.gz', timeout=60)
        response.raise_for_status()
        destination.write_bytes(response.content)

    @report
    def run(self) -> Self:
        # Prepare destinations
        check_destination(self.studies_local_path, delete=True)

        logger.debug('requesting crispr brain screens payload')
        screens_gz = self._post_screens()

        study_ids, screens_dict = self._extract_study_ids_from_screens_gz(screens_gz)
        logger.info(f'found {len(study_ids)} studies in screens payload')

        # Optionally validate API version
        if self.spec.validate_version is not None:
            version = int(screens_dict.get('__version', -1))
            if version != int(self.spec.validate_version):
                raise CrisprBrainError(f'api version mismatch: expected {self.spec.validate_version}, got {version}')

        # Save the raw gzipped screens payload
        self.screens_local_path.write_bytes(screens_gz)

        # Download each study CSV.gz
        session = requests.session()
        for study_id in study_ids:
            self._download_study_csv_gz(session, study_id, self.studies_local_path / f'{study_id}.csv.gz')

        logger.debug('downloaded all study files')

        # Upload results if remote storage configured
        if self.context.config.release_uri:
            logger.debug(f'uploading screens to {self.screens_remote_uri}')
            remote_storage = get_remote_storage(self.screens_remote_uri)
            remote_storage.upload(self.screens_local_path, self.screens_remote_uri)

            logger.debug(f'uploading {len(study_ids)} study files to {self.studies_remote_uri}')
            remote_storage = get_remote_storage(self.studies_remote_uri)
            # upload each file under the directory
            for file_path in self.studies_local_path.glob('*.csv.gz'):
                logger.debug(f'uploading {file_path.name}')
                remote_storage.upload(file_path, f'{self.studies_remote_uri}/{file_path.name}')

        self.artifacts = [
            Artifact(
                source=self.spec.api_url,
                destination=(self.screens_remote_uri or str(self.screens_local_path)),
            ),
            *[
                Artifact(
                    source=f'{self.spec.study_prefix_url}{study_id}.csv.gz',
                    destination=f'{self.studies_remote_uri or str(self.studies_local_path)}/{study_id}.csv.gz',
                )
                for study_id in study_ids
            ],
        ]
        return self

    @report
    def validate(self) -> Self:
        v(at_least_one_file_exist, self.studies_local_path, '*.csv.gz')
        return self

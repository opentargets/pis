"""Fetch CRISPR Brain screens metadata and study result files.

This task downloads the CRISPR Brain screens JSON (gzipped response) and all
per-study hit lists (CSV.gz).
"""

from __future__ import annotations

import gzip
import json
from typing import Any, Self
from urllib.parse import quote

import requests
from loguru import logger
from otter.manifest.model import Artifact
from otter.storage.synchronous.handle import StorageHandle
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError


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

        self.dst_screens = f'{spec.destination}/brain_screens.json.gz'
        self.dst_studies = f'{spec.destination}/brain_studies'

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

    @report
    def run(self) -> Self:
        logger.info('requesting crispr brain screens payload')

        screens_gz = self._post_screens()
        h_screens = StorageHandle(self.dst_screens, self.context.config)
        h_screens.write(screens_gz)
        self.artifacts: list[Artifact] = [Artifact(source=self.spec.api_url, destination=h_screens.absolute)]

        study_ids, screens_dict = self._extract_study_ids_from_screens_gz(screens_gz)
        logger.info(f'found {len(study_ids)} studies in screens payload')

        # Optionally validate API version
        if self.spec.validate_version is not None:
            version = int(screens_dict.get('__version', -1))
            if version != int(self.spec.validate_version):
                raise CrisprBrainError(f'api version mismatch: expected {self.spec.validate_version}, got {version}')

        # Download each study csv.gz
        session = requests.session()
        for study_id in study_ids:
            src = f'{self.spec.study_prefix_url}{quote(study_id)}.csv.gz'
            h_study = StorageHandle(f'{self.dst_studies}/{study_id}.csv.gz', self.context.config)
            dst = h_study.open('wb')
            response = session.get(src, timeout=60)
            response.raise_for_status()
            dst.write(response.content)
            self.artifacts.append(Artifact(source=src, destination=h_study.absolute))

        logger.info(f'downloaded {len(study_ids)} study files')
        return self

"""Fetch CRISPR Brain screens metadata and study result files.

This task downloads the CRISPR Brain screens JSON (gzipped response) and all
per-study hit lists (CSV.gz).
"""

from __future__ import annotations

import gzip
import http.client as http_client
import io
import json
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote, urlencode, urlparse

from loguru import logger
from otter.manifest.model import Artifact
from otter.storage import get_remote_storage
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report
from otter.util.errors import OtterError
from otter.util.fs import check_destination


class CrisprBrainError(OtterError):
    """Base class for CRISPR Brain fetch errors."""


class CrisprBrainSpec(Spec):
    """Configuration for the CRISPR Brain fetch task.

    Custom fields:
        - destination_screens (str): Path to save the API response (JSON.GZ).
        - destination_studies_dir (str): Directory to save per-study CSV.GZ files.
        - api_url (str): Full URL for the screens endpoint.
        - client_id (str): API client identifier.
        - study_prefix_url (str): Base URL for per-study CSV.GZ files.
        - validate_version (int | None): If provided, ensure API __version matches.
    """

    destination_screens: str
    destination_studies_dir: str
    api_url: str = 'https://crisprbrain.org/api/screens'
    client_id: str = '92f263647c43525d3e4f181aa7e348f26e32129c0f827321d9261cc9765c56c0'
    study_prefix_url: str = 'https://storage.googleapis.com/crisprbrain.appspot.com/api-data/'
    validate_version: int | None = 1


class CrisprBrain(Task):
    """Fetch CRISPR Brain screens metadata and per-study result files."""

    def __init__(self, spec: CrisprBrainSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: CrisprBrainSpec

        # Resolve local destinations
        self.screens_local_path: Path = context.config.work_path / spec.destination_screens
        self.studies_local_dir: Path = context.config.work_path / spec.destination_studies_dir

        # Resolve remote destinations if configured
        self.screens_remote_uri: str | None = None
        self.studies_remote_uri: str | None = None
        if context.config.release_uri:
            self.screens_remote_uri = f'{context.config.release_uri}/{spec.destination_screens}'
            self.studies_remote_uri = f'{context.config.release_uri}/{spec.destination_studies_dir}'

    def _post_screens(self) -> bytes:
        """POST to screens endpoint and return raw gzipped content."""
        payload = urlencode({'client_id': self.spec.client_id}).encode()
        parsed = urlparse(self.spec.api_url)
        if parsed.scheme not in {'https', 'http'} or not parsed.netloc:
            raise CrisprBrainError(f'invalid api_url: {self.spec.api_url}')
        connection_class = http_client.HTTPSConnection if parsed.scheme == 'https' else http_client.HTTPConnection
        path = parsed.path or '/'
        if parsed.query:
            path = f'{path}?{parsed.query}'

        try:
            conn = connection_class(parsed.netloc, timeout=60)
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            conn.request('POST', path, body=payload, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            data = resp.read()
            conn.close()
        except OSError as e:
            raise CrisprBrainError(f'error calling screens API: {e}') from e

        if status != 200:
            raise CrisprBrainError(f'screens API returned HTTP {status}')
        return data

    @staticmethod
    def _extract_study_ids_from_screens_gz(screens_gz: bytes) -> tuple[list[str], dict[str, Any]]:
        """Extract study identifiers and parsed JSON from gzipped screens payload.

        Returns a tuple of (study_ids, parsed_json_dict).
        """
        try:
            json_text = gzip.decompress(screens_gz).decode()
            screens = json.loads(json_text)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
            raise CrisprBrainError(f'error parsing screens payload: {e}') from e

        # Flatten values; each study entry is a dict with metadata
        # The special keys (like __version) are not dicts and are skipped
        study_entries: list[dict[str, Any]] = [
            {**value, **value.get('metadata', {})} for value in screens.values() if isinstance(value, dict)
        ]

        # Use the same field as the original Spark code: "Screen Name"
        study_ids = [entry['Screen Name'] for entry in study_entries if 'Screen Name' in entry]
        return study_ids, screens

    def _download_study_csv_gz(self, study_id: str, destination: Path) -> None:
        url = f'{self.spec.study_prefix_url}{quote(study_id)}.csv.gz'
        parsed = urlparse(url)
        if parsed.scheme not in {'https', 'http'} or not parsed.netloc:
            raise CrisprBrainError(f'invalid study URL: {url}')
        connection_class = http_client.HTTPSConnection if parsed.scheme == 'https' else http_client.HTTPConnection
        path = parsed.path or '/'
        if parsed.query:
            path = f'{path}?{parsed.query}'
        try:
            conn = connection_class(parsed.netloc, timeout=60)
            conn.request('GET', path)
            resp = conn.getresponse()
            status = resp.status
            data = resp.read()
            conn.close()
        except OSError as e:
            raise CrisprBrainError(f'error downloading study {study_id}: {e}') from e
        if status != 200:
            raise CrisprBrainError(f'study file for {study_id} returned HTTP {status}')

        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.write_bytes(data)
        except OSError as e:
            raise CrisprBrainError(f'error writing study {study_id} to {destination}: {e}') from e

    @report
    def run(self) -> Self:
        # Prepare destinations
        check_destination(self.screens_local_path, delete=True)
        self.studies_local_dir.mkdir(parents=True, exist_ok=True)

        logger.debug('requesting CRISPR Brain screens payload')
        screens_gz = self._post_screens()

        # Optionally validate API version
        study_ids, screens_dict = self._extract_study_ids_from_screens_gz(screens_gz)
        logger.info(f'found {len(study_ids)} studies in screens payload')

        if self.spec.validate_version is not None:
            version = int(screens_dict.get('__version', -1))
            if version != int(self.spec.validate_version):
                raise CrisprBrainError(
                    f"API versions don't match: expected {self.spec.validate_version}, got {version}"
                )

        # Save the raw gzipped screens payload
        try:
            self.screens_local_path.write_bytes(screens_gz)
        except OSError as e:
            raise CrisprBrainError(f'error writing screens file: {e}') from e

        # Download each study CSV.gz
        for study_id in study_ids:
            study_path = self.studies_local_dir / f'{study_id}.csv.gz'
            self._download_study_csv_gz(study_id, study_path)

        logger.debug('downloaded all study files')

        # Upload results if remote storage configured
        if self.screens_remote_uri:
            remote_storage = get_remote_storage(self.screens_remote_uri)
            remote_storage.upload(self.screens_local_path, self.screens_remote_uri)

        if self.studies_remote_uri:
            remote_storage = get_remote_storage(self.studies_remote_uri)
            # upload each file under the directory
            for file_path in self.studies_local_dir.glob('*.csv.gz'):
                rel_name = file_path.name
                remote_storage.upload(file_path, f'{self.studies_remote_uri}/{rel_name}')

        self.artifacts = [
            Artifact(
                source=self.spec.api_url,
                destination=(self.screens_remote_uri or str(self.screens_local_path)),
            ),
            Artifact(
                source=f'{self.spec.study_prefix_url}{{studyId}}.csv.gz',
                destination=(self.studies_remote_uri or str(self.studies_local_dir)),
            ),
        ]
        return self

    @report
    def validate(self) -> Self:
        # Basic validation: ensure files exist and are readable
        try:
            # screens file must be gzipped json
            raw = self.screens_local_path.read_bytes()
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                json.load(io.TextIOWrapper(gz))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
            raise CrisprBrainError(f'validation failed for screens file: {e}') from e

        # at least one study file must exist
        if not any(self.studies_local_dir.glob('*.csv.gz')):
            raise CrisprBrainError('no study files were downloaded')

        return self

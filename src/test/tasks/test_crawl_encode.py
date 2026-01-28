"""Tests for the CrawlEncode task."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from otter.task.model import TaskContext
from otter.tasks.copy import CopySpec

from pis.tasks.crawl_encode import (
    CrawlEncode,
    CrawlEncodeSpec,
    EncodeManifestMissingValueError,
    EncodeManifestSchema,
    EncodeManifestSchemaValidationError,
)

URL_STEM_COL = 'File download URL'
ACCESSION_COL = 'Accession'
SEARCH_PARAMS = '# Search parameters: type=Experiment&status=released'


class TestParseManifest:
    """Tests for CrawlEncode._parse_manifest static method."""

    @pytest.fixture
    def manifest_file(self, tmp_path: Path) -> Path:
        """Return path to temporary manifest file."""
        return tmp_path / 'manifest.tsv'

    def _write_manifest(self, path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
        """Write a TSV manifest file with search parameters header."""
        lines = [SEARCH_PARAMS]
        if header:
            lines.append('\t'.join(header))
        lines.extend('\t'.join(row.get(col, '') for col in header) for row in rows)
        path.write_text('\n'.join(lines), encoding='utf-8')

    @pytest.mark.parametrize(
        ('header', 'rows', 'expected'),
        [
            pytest.param(
                [URL_STEM_COL, ACCESSION_COL],
                [
                    {URL_STEM_COL: '/files/ENCFF001/@@download/ENCFF001.bed.gz', ACCESSION_COL: 'ENCFF001'},
                    {URL_STEM_COL: '/files/ENCFF002/@@download/ENCFF002.bed.gz', ACCESSION_COL: 'ENCFF002'},
                ],
                {
                    'ENCFF001': '/files/ENCFF001/@@download/ENCFF001.bed.gz',
                    'ENCFF002': '/files/ENCFF002/@@download/ENCFF002.bed.gz',
                },
                id='valid_manifest_with_data',
            ),
            pytest.param(
                [URL_STEM_COL, ACCESSION_COL, 'Extra Column'],
                [
                    {
                        URL_STEM_COL: '/files/ENCFF003/@@download/ENCFF003.bed.gz',
                        ACCESSION_COL: 'ENCFF003',
                        'Extra Column': 'ignored',
                    },
                ],
                {
                    'ENCFF003': '/files/ENCFF003/@@download/ENCFF003.bed.gz',
                },
                id='valid_manifest_with_extra_columns',
            ),
        ],
    )
    def test_parse_manifest_success(
        self,
        manifest_file: Path,
        header: list[str],
        rows: list[dict[str, str]],
        expected: dict[str, str],
    ) -> None:
        """Test successful parsing of valid manifest files."""
        self._write_manifest(manifest_file, header, rows)

        result = EncodeManifestSchema(url_stem=URL_STEM_COL, accession=ACCESSION_COL).parse_manifest(manifest_file)

        assert result == expected

    @pytest.mark.parametrize(
        ('header', 'rows', 'missing_col'),
        [
            pytest.param(
                [ACCESSION_COL],
                [{ACCESSION_COL: 'ENCFF001'}],
                URL_STEM_COL,
                id='missing_url_stem_column',
            ),
            pytest.param(
                [URL_STEM_COL],
                [{URL_STEM_COL: '/files/ENCFF001/@@download/ENCFF001.bed.gz'}],
                ACCESSION_COL,
                id='missing_accession_column',
            ),
        ],
    )
    def test_parse_manifest_missing_column(
        self,
        manifest_file: Path,
        header: list[str],
        rows: list[dict[str, str]],
        missing_col: str,
    ) -> None:
        """Test error when required column is missing from manifest."""
        self._write_manifest(manifest_file, header, rows)

        with pytest.raises(
            EncodeManifestSchemaValidationError, match=f'Manifest file is missing required column.*{missing_col}'
        ):
            EncodeManifestSchema(url_stem=URL_STEM_COL, accession=ACCESSION_COL).parse_manifest(manifest_file)

    @pytest.mark.parametrize(
        ('header', 'rows'),
        [
            pytest.param(
                [URL_STEM_COL, ACCESSION_COL],
                [
                    {URL_STEM_COL: '', ACCESSION_COL: 'ENCFF001'},
                ],
                id='missing_url_stem_value',
            ),
            pytest.param(
                [URL_STEM_COL, ACCESSION_COL],
                [
                    {URL_STEM_COL: '/files/ENCFF001/@@download/ENCFF001.bed.gz', ACCESSION_COL: ''},
                ],
                id='missing_accession_value',
            ),
            pytest.param(
                [URL_STEM_COL, ACCESSION_COL],
                [
                    {URL_STEM_COL: '/files/ENCFF001/@@download/ENCFF001.bed.gz', ACCESSION_COL: 'ENCFF001'},
                    {URL_STEM_COL: '', ACCESSION_COL: ''},
                ],
                id='missing_both_values_in_second_row',
            ),
        ],
    )
    def test_parse_manifest_missing_data(
        self,
        manifest_file: Path,
        header: list[str],
        rows: list[dict[str, str]],
    ) -> None:
        """Test error when required data values are missing in rows."""
        self._write_manifest(manifest_file, header, rows)
        with pytest.raises(EncodeManifestMissingValueError, match='Missing required value in manifest row'):
            EncodeManifestSchema(url_stem=URL_STEM_COL, accession=ACCESSION_COL).parse_manifest(manifest_file)

    def test_parse_manifest_empty_file(self, manifest_file: Path) -> None:
        """Test error when manifest file has no header (only search params)."""
        manifest_file.write_text(SEARCH_PARAMS, encoding='utf-8')

        with pytest.raises(EncodeManifestSchemaValidationError, match='Manifest file is empty or malformed'):
            EncodeManifestSchema(url_stem=URL_STEM_COL, accession=ACCESSION_COL).parse_manifest(manifest_file)


class TestCrawlEncodeIntegration:
    """Integration tests for CrawlEncode task."""

    @pytest.fixture
    def manifest_file(self, tmp_path: Path) -> Path:
        """Create a test manifest file."""
        manifest = tmp_path / 'manifest.tsv'
        lines = [
            SEARCH_PARAMS,
            '\t'.join([URL_STEM_COL, ACCESSION_COL]),
            '/files/ENCFF001/@@download/ENCFF001.bed.gz\tENCFF001',
            '/files/ENCFF002/@@download/ENCFF002.bed.gz\tENCFF002',
        ]
        manifest.write_text('\n'.join(lines), encoding='utf-8')
        return manifest

    @pytest.fixture
    def task_context(self, tmp_path: Path) -> TaskContext:
        """Create a mock TaskContext."""
        context = MagicMock()
        context.config = MagicMock()
        context.config.work_path = tmp_path
        context.config.release_uri = None
        return context

    @pytest.fixture
    def crawl_encode_spec(self, manifest_file: Path) -> CrawlEncodeSpec:
        """Create a CrawlEncodeSpec for testing."""
        return CrawlEncodeSpec(
            name='crawl_encode test',
            manifest=manifest_file.name,
            columns=EncodeManifestSchema(url_stem=URL_STEM_COL, accession=ACCESSION_COL),
            expand=CopySpec(
                name='copy encode files',
                source='https://www.encodeproject.org${url_stem}',
                destination='encode_files/${accession}.bed.gz',
            ),
        )

    def test_crawl_encode_run_transfer_all_with_local_fs(
        self,
        tmp_path: Path,
        task_context: TaskContext,
        crawl_encode_spec: CrawlEncodeSpec,
    ) -> None:
        """Test CrawlEncode.run() transfers files to local path."""
        task_context.config.work_path = tmp_path

        task = CrawlEncode(crawl_encode_spec, task_context)

        with patch.object(CrawlEncode, '_transfer_all', new_callable=AsyncMock) as mock_transfer:
            mock_transfer.return_value = None
            task.run()

            # Verify that _transfer_all was called with correct specs
            mock_transfer.assert_called_once()
            specs = mock_transfer.call_args[0][0]
            assert len(specs) == 2
            assert specs[0].destination == str(tmp_path / 'encode_files' / 'ENCFF001.bed.gz')
            assert specs[1].destination == str(tmp_path / 'encode_files' / 'ENCFF002.bed.gz')

    def test_crawl_encode_run_transfer_all_with_gcs(
        self,
        task_context: TaskContext,
        crawl_encode_spec: CrawlEncodeSpec,
    ) -> None:
        """Test CrawlEncode.run() transfers files to GCS path."""
        task_context.config.release_uri = 'gs://my-bucket/releases'

        task = CrawlEncode(crawl_encode_spec, task_context)

        with patch.object(CrawlEncode, '_transfer_all', new_callable=AsyncMock) as mock_transfer:
            mock_transfer.return_value = None
            task.run()

            # Verify that _transfer_all was called with correct GCS specs
            mock_transfer.assert_called_once()
            specs = mock_transfer.call_args[0][0]
            assert len(specs) == 2
            assert specs[0].destination == 'gs://my-bucket/releases/encode_files/ENCFF001.bed.gz'
            assert specs[1].destination == 'gs://my-bucket/releases/encode_files/ENCFF002.bed.gz'

"""Tests for the CrawlEncode task."""

from pathlib import Path

import pytest

from pis.tasks.crawl_encode import CrawlEncode, EncodeManifestMissingValuError

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

        result = CrawlEncode._parse_manifest(manifest_file, URL_STEM_COL, ACCESSION_COL)

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

        with pytest.raises(ValueError, match=f'Manifest file is missing required column.*{missing_col}'):
            CrawlEncode._parse_manifest(manifest_file, URL_STEM_COL, ACCESSION_COL)  # noqa: SLF001

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

        with pytest.raises(EncodeManifestMissingValuError, match='Missing required value in manifest row'):
            CrawlEncode._parse_manifest(manifest_file, URL_STEM_COL, ACCESSION_COL)  # noqa: SLF001

    def test_parse_manifest_empty_file(self, manifest_file: Path) -> None:
        """Test error when manifest file has no header (only search params)."""
        manifest_file.write_text(SEARCH_PARAMS, encoding='utf-8')

        with pytest.raises(ValueError, match='Manifest file is empty or malformed'):
            CrawlEncode._parse_manifest(manifest_file, URL_STEM_COL, ACCESSION_COL)  # noqa: SLF001

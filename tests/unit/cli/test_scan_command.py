"""Unit tests for scan-candidates CLI command.

Tests cover:
- --params-json with valid JSON string
- --params-json with invalid JSON syntax
- --params-json with non-object JSON (array)
- --params-file with valid JSON file
- --params-file with invalid JSON file
- --params-file with non-object JSON file
- Conflicting --params-json and --params-file options
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qs_trader.cli.commands.scan import scan_candidates_command
from qs_trader.services.data.instrument_resolver import MinimalInstrument
from qs_trader.services.scan.runner import ScanSummary


@pytest.fixture
def cli_runner():
    """Fixture providing Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_scan_runner():
    """Fixture providing mock ScanRunner, ClickHouse client, and InstrumentResolver.

    The scan command imports ClickHouse client and InstrumentResolver inside
    the function body, so we patch them at their source modules.
    """
    with (
        patch("qs_trader.services.scan.runner.ScanRunner") as mock_runner_class,
        patch("clickhouse_connect.get_client") as mock_get_client,
        patch("qs_trader.services.data.instrument_resolver.InstrumentResolver") as mock_resolver_class,
    ):
        # Setup ScanRunner mock
        mock_runner_instance = MagicMock()
        mock_runner_instance.run.return_value = ([], ScanSummary())
        mock_runner_class.return_value = mock_runner_instance

        # Setup ClickHouse client mock
        mock_ch_client = MagicMock()
        mock_get_client.return_value = mock_ch_client

        # Setup InstrumentResolver mock
        mock_resolver = MagicMock()
        mock_resolver_class.return_value = mock_resolver

        yield mock_runner_class


# ---------------------------------------------------------------------------
# Test 9.8: CLI tests for JSON string parameters
# ---------------------------------------------------------------------------

class TestParamsJson:
    """Tests for --params-json CLI option."""

    def test_params_json_valid(self, cli_runner, mock_scan_runner):
        """Valid --params-json parses correctly and passes to runner."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-json", '{"lookback": 20}',
                ],
            )
            assert result.exit_code == 0
            assert "lookback" in result.output or "20" in result.output
            # Verify runner was instantiated with parameters
            mock_scan_runner.assert_called_once()
            call_kwargs = mock_scan_runner.call_args[1]
            assert call_kwargs.get("parameters") == {"lookback": 20}

    def test_params_json_invalid_syntax(self, cli_runner, mock_scan_runner):
        """Invalid JSON in --params-json exits with code 1 and clear error."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-json", "not json",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid --params-json" in result.output

    def test_params_json_not_object(self, cli_runner, mock_scan_runner):
        """JSON array in --params-json exits with code 1 and clear error."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-json", "[1, 2, 3]",
                ],
            )
            assert result.exit_code == 1
            assert "must be a JSON object" in result.output


# ---------------------------------------------------------------------------
# Test 9.9: CLI tests for JSON file parameters
# ---------------------------------------------------------------------------

class TestParamsFile:
    """Tests for --params-file CLI option."""

    def test_params_file_valid(self, cli_runner, mock_scan_runner):
        """Valid JSON file parses correctly and passes to runner."""
        with cli_runner.isolated_filesystem():
            params_file = Path("params.json")
            params_file.write_text('{"threshold": 0.5}')

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-file", str(params_file),
                ],
            )
            assert result.exit_code == 0
            assert "threshold" in result.output or "0.5" in result.output
            mock_scan_runner.assert_called_once()
            call_kwargs = mock_scan_runner.call_args[1]
            assert call_kwargs.get("parameters") == {"threshold": 0.5}

    def test_params_file_invalid_json(self, cli_runner, mock_scan_runner):
        """Invalid JSON in params file exits with code 1 and clear error."""
        with cli_runner.isolated_filesystem():
            params_file = Path("params.json")
            params_file.write_text("not json")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-file", str(params_file),
                ],
            )
            assert result.exit_code == 1
            assert "Invalid JSON in --params-file" in result.output

    def test_params_file_not_object(self, cli_runner, mock_scan_runner):
        """JSON array in params file exits with code 1 and clear error."""
        with cli_runner.isolated_filesystem():
            params_file = Path("params.json")
            params_file.write_text("[1, 2, 3]")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-file", str(params_file),
                ],
            )
            assert result.exit_code == 1
            assert "must contain a JSON object" in result.output


# ---------------------------------------------------------------------------
# Test 9.10: Conflicting parameter options
# ---------------------------------------------------------------------------

class TestParamsConflict:
    """Tests for conflicting --params-json and --params-file options."""

    def test_params_json_and_file_conflict(self, cli_runner, mock_scan_runner):
        """Using both --params-json and --params-file exits with code 1."""
        with cli_runner.isolated_filesystem():
            params_file = Path("params.json")
            params_file.write_text("{}")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "33449",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                    "--params-json", '{"lookback": 20}',
                    "--params-file", str(params_file),
                ],
            )
            assert result.exit_code == 1
            assert "Cannot use both" in result.output


# ---------------------------------------------------------------------------
# Test 2.1: --secid-list file-based secid loading
# ---------------------------------------------------------------------------


class TestSecidList:
    """Tests for --secid-list CLI option."""

    def test_secid_list_valid(self, cli_runner, mock_scan_runner):
        """Valid secid file loads secids and passes to runner."""
        with cli_runner.isolated_filesystem():
            secid_file = Path("secids.txt")
            secid_file.write_text("33449\n67890\n")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-list", str(secid_file),
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 0
            assert "Loaded 2 secids" in result.output

    def test_secid_list_with_comments_and_blanks(self, cli_runner, mock_scan_runner):
        """File with # comments and blank lines is parsed correctly."""
        with cli_runner.isolated_filesystem():
            secid_file = Path("secids.txt")
            secid_file.write_text(
                "# AAPL\n"
                "33449\n"
                "\n"
                "# MSFT\n"
                "67890\n"
                "\n"
                "# GOOGL\n"
                "11111\n"
            )

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-list", str(secid_file),
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 0
            assert "Loaded 3 secids" in result.output
            # Verify secids appear in display
            assert "33449" in result.output
            assert "67890" in result.output
            assert "11111" in result.output

    def test_secid_list_combined_with_secid(self, cli_runner, mock_scan_runner):
        """--secid-list and --secid options combine secids."""
        with cli_runner.isolated_filesystem():
            secid_file = Path("secids.txt")
            secid_file.write_text("33449\n67890\n")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid", "99999",
                    "--secid-list", str(secid_file),
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 0
            assert "Loaded 2 secids" in result.output
            assert "33449" in result.output
            assert "67890" in result.output
            assert "99999" in result.output

    def test_secid_list_file_not_found(self, cli_runner, mock_scan_runner):
        """Non-existent file exits with code 2 (Click usage error)."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-list", "nonexistent.txt",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 2
            assert "nonexistent.txt" in result.output

    def test_secid_list_invalid_integer(self, cli_runner, mock_scan_runner):
        """Non-integer content exits with code 1 and clear error."""
        with cli_runner.isolated_filesystem():
            secid_file = Path("secids.txt")
            secid_file.write_text("33449\nnot_a_number\n67890\n")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-list", str(secid_file),
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 1
            assert "Invalid secid" in result.output
            assert "not_a_number" in result.output


# ---------------------------------------------------------------------------
# Test 2.2: --secid-all and --universe-as-of-date CLI options
# ---------------------------------------------------------------------------


class TestSecidAll:
    """Tests for --secid-all and --universe-as-of-date CLI options."""

    def test_secid_all_resolves_and_scans(self, cli_runner, mock_scan_runner):
        """--secid-all with --universe-as-of-date resolves all secids and scans."""
        from qs_trader.services.data.instrument_resolver import InstrumentResolver

        mock_resolver = InstrumentResolver.return_value
        mock_resolver.resolve_all_secids.return_value = [
            MinimalInstrument(secid=1, display_symbol="AAPL"),
            MinimalInstrument(secid=2, display_symbol="MSFT"),
        ]

        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-all",
                    "--universe-as-of-date", "2024-01-01",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 0
            assert "Resolved 2 active instruments" in result.output
            mock_resolver.resolve_all_secids.assert_called_once()

    def test_secid_all_without_universe_date_fails(self, cli_runner, mock_scan_runner):
        """--secid-all without --universe-as-of-date exits with code 1."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-all",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 1
            assert "--secid-all requires --universe-as-of-date" in result.output

    def test_secid_all_mutually_exclusive_with_secid(self, cli_runner, mock_scan_runner):
        """--secid-all with --secid exits with code 1."""
        with cli_runner.isolated_filesystem():
            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-all",
                    "--secid", "12345",
                    "--universe-as-of-date", "2024-01-01",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 1
            assert "mutually exclusive with --secid" in result.output

    def test_secid_all_mutually_exclusive_with_secid_list(self, cli_runner, mock_scan_runner):
        """--secid-all with --secid-list exits with code 1."""
        with cli_runner.isolated_filesystem():
            secid_file = Path("secids.txt")
            secid_file.write_text("12345\n")

            result = cli_runner.invoke(
                scan_candidates_command,
                [
                    "--secid-all",
                    "--secid-list", str(secid_file),
                    "--universe-as-of-date", "2024-01-01",
                    "--start-date", "2024-01-01",
                    "--end-date", "2024-01-31",
                ],
            )
            assert result.exit_code == 1
            assert "mutually exclusive with --secid-list" in result.output

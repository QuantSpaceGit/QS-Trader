"""Regression tests for HTML report metadata rendering."""

from pathlib import Path

from qs_trader.services.reporting.html_reporter import HTMLReportGenerator


def test_build_run_info_supports_versioned_metadata_shape(tmp_path: Path) -> None:
    """HTML report should render configuration from metadata.json version 1.1."""
    generator = HTMLReportGenerator(tmp_path)

    html = generator._build_run_info(
        performance={
            "initial_equity": 100000,
            "final_equity": 110000,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "duration_days": 365,
        },
        manifest=None,
        metadata={
            "metadata_version": "1.1",
            "backtest": {
                "submitted_config": {
                    "backtest_id": "versioned-run",
                    "start_date": "2024-01-01T00:00:00",
                    "end_date": "2024-12-31T00:00:00",
                    "data": {
                        "sources": [{"name": "qs-datamaster-equity-1d", "universe": ["AAPL"]}],
                    },
                },
                "effective_execution_spec": {
                    "schema_version": 1,
                    "captured_from": "qs_trader.reporting",
                    "strategy_adjustment_mode": "split_adjusted",
                    "portfolio_adjustment_mode": "split_adjusted",
                    "risk_policy": {"name": "naive", "effective_config": {}},
                    "strategies": [],
                },
            },
        },
        config_snapshot=None,
    )

    assert "⚙️ Configuration" in html
    assert "versioned-run" in html
    assert "Requested Start" in html
    assert "Requested End" in html
    assert "qs-datamaster-equity-1d" in html
    assert "split_adjusted" in html
    assert "qs_trader.reporting" in html
    assert "v1" in html
    assert "naive" in html


def test_build_run_info_uses_effective_strategy_params_when_available(tmp_path: Path) -> None:
    """HTML report should surface effective runtime params from versioned metadata."""
    generator = HTMLReportGenerator(tmp_path)

    html = generator._build_run_info(
        performance={
            "initial_equity": 100000,
            "final_equity": 110000,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "duration_days": 365,
        },
        manifest=None,
        metadata={
            "metadata_version": "1.1",
            "backtest": {
                "submitted_config": {
                    "strategies": [
                        {
                            "strategy_id": "buy_and_hold",
                            "config": {},
                            "universe": ["MSFT"],
                            "data_sources": ["qs-datamaster-equity-1d"],
                        }
                    ]
                },
                "effective_execution_spec": {
                    "schema_version": 1,
                    "captured_from": "qs_trader.reporting",
                    "strategies": [
                        {
                            "strategy_id": "buy_and_hold",
                            "effective_params": {
                                "rebalance_days": 20,
                                "allow_fractional": False,
                            },
                            "universe": ["MSFT"],
                            "data_sources": ["qs-datamaster-equity-1d"],
                        }
                    ],
                },
            },
        },
        config_snapshot=None,
    )

    assert "🎯 Strategy &amp; Universe" not in html  # sanity check: emoji remains literal HTML text
    assert "🎯 Strategy & Universe" in html
    assert "buy_and_hold" in html
    assert "MSFT" in html
    assert "Rebalance Days" in html
    assert "Allow Fractional" in html
    assert ">20</span>" in html
    assert ">False</span>" in html


def test_build_run_info_preserves_legacy_metadata_shape(tmp_path: Path) -> None:
    """Legacy metadata.json version 1.0 should still render configuration."""
    generator = HTMLReportGenerator(tmp_path)

    html = generator._build_run_info(
        performance={
            "initial_equity": 100000,
            "final_equity": 105000,
            "start_date": "2024-01-01",
            "end_date": "2024-06-30",
            "duration_days": 181,
        },
        manifest=None,
        metadata={
            "metadata_version": "1.0",
            "backtest": {
                "backtest_id": "legacy-run",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-06-30T00:00:00",
                "strategy_adjustment_mode": "raw",
                "portfolio_adjustment_mode": "raw",
                "risk_policy": {"name": "naive"},
            },
        },
        config_snapshot=None,
    )

    assert "⚙️ Configuration" in html
    assert "legacy-run" in html
    assert "raw" in html
    assert "naive" in html

"""Focused unit tests for the optional backtest sleeve block."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from qs_trader.engine.config import BacktestConfig


def _base_config() -> dict:
    return {
        "backtest_id": "sleeve_test",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "initial_equity": "100000",
        "data": {"sources": [{"name": "ds1", "universe": ["AAPL"]}]},
        "strategies": [
            {
                "strategy_id": "sma_crossover",
                "universe": ["AAPL"],
                "data_sources": ["ds1"],
                "config": {"fast_period": 10},
            }
        ],
        "risk_policy": {"name": "naive", "config": {}},
    }


def test_backtest_config_parses_optional_sleeve_block_and_exposes_runtime_budget() -> None:
    raw = {
        **_base_config(),
        "sleeve": {
            "sleeve_id": "sma_crossover:AAPL",
            "sleeve_key": "AAPL",
            "allocated_equity": "25000",
            "symbol": "AAPL",
        },
    }

    config = BacktestConfig(**raw)

    assert config.sleeve is not None
    assert config.sleeve.sleeve_id == "sma_crossover:AAPL"
    assert config.sleeve.parsed_sleeve_id.strategy_id == "sma_crossover"
    assert config.sleeve_budget is not None
    assert config.sleeve_budget.allocated_equity == Decimal("25000")
    assert config.sleeve_budget.symbols == ("AAPL",)


def test_sleeve_bound_config_requires_single_loaded_symbol() -> None:
    raw = _base_config()
    raw["data"] = {"sources": [{"name": "ds1", "universe": ["AAPL", "MSFT"]}]}
    raw["strategies"][0]["universe"] = ["AAPL", "MSFT"]
    raw["sleeve"] = {
        "sleeve_id": "sma_crossover:AAPL",
        "sleeve_key": "AAPL",
        "allocated_equity": "25000",
        "symbol": "AAPL",
    }

    with pytest.raises(ValidationError, match="Sleeve-bound backtests must load exactly one symbol"):
        BacktestConfig(**raw)


def test_sleeve_bound_config_requires_strategy_declared_in_sleeve_id() -> None:
    raw = {
        **_base_config(),
        "sleeve": {
            "sleeve_id": "other_strategy:AAPL",
            "sleeve_key": "AAPL",
            "allocated_equity": "25000",
            "symbol": "AAPL",
        },
    }

    with pytest.raises(ValidationError, match="sleeve_id strategy 'other_strategy' not found"):
        BacktestConfig(**raw)


def test_multi_symbol_configs_remain_valid_without_sleeve_block() -> None:
    raw = _base_config()
    raw["data"] = {"sources": [{"name": "ds1", "universe": ["AAPL", "MSFT"]}]}
    raw["strategies"][0]["universe"] = ["AAPL", "MSFT"]

    config = BacktestConfig(**raw)

    assert config.sleeve is None
    assert config.sleeve_budget is None
    assert config.all_symbols == {"AAPL", "MSFT"}

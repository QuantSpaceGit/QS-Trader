"""Unit tests for effective execution provenance capture."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

from qs_trader.services.reporting.service import build_effective_execution_spec


class _DummyStrategyConfig:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def model_dump(self, *, mode: str = "json") -> dict[str, object]:
        return dict(self._payload)


class _DummyManagerService:
    def __init__(self, effective_risk_config: dict[str, object]) -> None:
        self._effective_risk_config = effective_risk_config

    def get_effective_risk_config(self) -> dict[str, object]:
        return dict(self._effective_risk_config)


def test_build_effective_execution_spec_captures_resolved_strategy_and_risk_config() -> None:
    """Resolved runtime defaults should be captured independently from submitted config."""
    backtest_config = SimpleNamespace(
        strategies=[
            SimpleNamespace(
                strategy_id="sma_crossover",
                config={"fast_period": 10},
                universe=["AAPL"],
                data_sources=["qs-datamaster-equity-1d"],
            )
        ],
        risk_policy=SimpleNamespace(name="naive", config={}),
        strategy_adjustment_mode="split_adjusted",
        portfolio_adjustment_mode="split_adjusted",
    )
    strategy_instances = {
        "sma_crossover": SimpleNamespace(
            strategy_id="sma_crossover",
            symbols=["AAPL"],
            data_sources=["qs-datamaster-equity-1d"],
            config=_DummyStrategyConfig(
                {
                    "name": "SMA Crossover",
                    "fast_period": 10,
                    "slow_period": 50,
                    "signal_period": 5,
                    "universe": ["AAPL"],
                }
            ),
        )
    }
    manager_service = _DummyManagerService(
        {
            "budgets": [{"strategy_id": "sma_crossover", "capital_weight": 1.0}],
            "sizing": {
                "sma_crossover": {
                    "model": "fixed_fraction",
                    "fraction": Decimal("0.02"),
                    "min_quantity": 1,
                    "lot_size": 1,
                }
            },
            "concentration": {"max_position_pct": 0.1},
            "leverage": {"max_gross": 1.0, "max_net": 1.0},
            "shorting": {"allow_short_positions": False},
            "cash_buffer_pct": 0.02,
        }
    )

    payload = build_effective_execution_spec(
        backtest_config=backtest_config,
        strategy_instances=cast(dict[str, Any], strategy_instances),
        manager_service=cast(Any, manager_service),
    )

    assert payload == {
        "schema_version": 1,
        "captured_from": "qs_trader.reporting",
        "strategies": [
            {
                "strategy_id": "sma_crossover",
                "effective_params": {
                    "fast_period": 10,
                    "slow_period": 50,
                    "signal_period": 5,
                },
                "universe": ["AAPL"],
                "data_sources": ["qs-datamaster-equity-1d"],
            }
        ],
        "risk_policy": {
            "name": "naive",
            "effective_config": {
                "budgets": [{"strategy_id": "sma_crossover", "capital_weight": 1.0}],
                "sizing": {
                    "sma_crossover": {
                        "model": "fixed_fraction",
                        "fraction": Decimal("0.02"),
                        "min_quantity": 1,
                        "lot_size": 1,
                    }
                },
                "concentration": {"max_position_pct": 0.1},
                "leverage": {"max_gross": 1.0, "max_net": 1.0},
                "shorting": {"allow_short_positions": False},
                "cash_buffer_pct": 0.02,
            },
        },
        "strategy_adjustment_mode": "split_adjusted",
        "portfolio_adjustment_mode": "split_adjusted",
    }


def test_build_effective_execution_spec_preserves_resolved_defaults_when_submission_is_empty() -> None:
    """Empty submitted overrides should still persist the runtime defaults that executed."""
    backtest_config = SimpleNamespace(
        strategies=[
            SimpleNamespace(
                strategy_id="buy_and_hold",
                config={},
                universe=["MSFT"],
                data_sources=["qs-datamaster-equity-1d"],
            )
        ],
        risk_policy=SimpleNamespace(name="naive", config={}),
        strategy_adjustment_mode="split_adjusted",
        portfolio_adjustment_mode="split_adjusted",
    )
    strategy_instances = {
        "buy_and_hold": SimpleNamespace(
            strategy_id="buy_and_hold",
            config=_DummyStrategyConfig(
                {
                    "name": "Buy and Hold",
                    "rebalance_days": 20,
                    "allow_fractional": False,
                }
            ),
        )
    }

    payload = build_effective_execution_spec(
        backtest_config=backtest_config,
        strategy_instances=cast(dict[str, Any], strategy_instances),
        manager_service=None,
    )

    assert payload["strategies"] == [
        {
            "strategy_id": "buy_and_hold",
            "effective_params": {
                "rebalance_days": 20,
                "allow_fractional": False,
            },
            "universe": ["MSFT"],
            "data_sources": ["qs-datamaster-equity-1d"],
        }
    ]

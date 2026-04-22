"""
Backtest Configuration Models.

Philosophy: Clean separation of concerns
- qs_trader.yaml: ALL service configurations (execution, risk, portfolio, data, etc.)
- backtest YAML: ONLY run parameters (dates, universe, capital) + strategies

This module provides BacktestConfig for per-run parameters.
Services get their configuration from SystemConfig (qs_trader.yaml).
"""

import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from qs_trader.events.price_basis import PriceBasis


class DataSourceConfig(BaseModel):
    """Configuration for a single data source with its universe.

    Allows specifying different symbols for different data sources.
    Example: Load AAPL prices from one source, AAPL news from another.
    """

    name: str = Field(..., description="Data source name from data_sources.yaml")
    universe: list[str] = Field(..., description="Symbols to load from this data source")


class DataSelectionConfig(BaseModel):
    """Data selection configuration for backtest run.

    Specifies WHAT data to load for this specific backtest:
    - Which data sources to use
    - Which symbols to load from each source

    Data source details (adapter, path, etc.) are defined in data_sources.yaml.
    System-wide data handling preferences come from SystemConfig.DataServiceConfig.
    """

    sources: list[DataSourceConfig] = Field(
        ..., min_length=1, description="Data sources with their universes (at least one required)"
    )


class StrategyConfigItem(BaseModel):
    """Configuration for a single strategy.

    Strategies are referenced by registry name (not file path).
    System loads custom strategies from custom_libraries.strategies path.
    """

    strategy_id: str = Field(..., description="Strategy name from registry (buildin or custom)")
    universe: list[str] = Field(..., description="Symbols this strategy trades (must be subset of backtest universe)")
    data_sources: list[str] = Field(
        ...,
        description="Data sources this strategy uses (e.g., ['yahoo-us-equity-1d-csv', 'news-feed'])",
    )
    config: dict[str, Any] = Field(default_factory=dict, description="Strategy-specific config overrides")


class RiskPolicyConfig(BaseModel):
    """Risk policy configuration.

    Risk policies are referenced by registry name (buildin or custom).
    Applied at Portfolio Manager level, not strategy level.
    """

    name: str = Field(..., description="Risk policy name from registry")
    config: dict[str, Any] = Field(default_factory=dict, description="Policy-specific config overrides")


class ReportingConfigItem(BaseModel):
    """Reporting configuration for backtest run.

    Controls performance metrics calculation, output formats, and console display.
    All fields are optional with sensible defaults.

    This configuration is at the portfolio/backtest run level, not strategy or system level.
    """

    # Event emission during backtest
    emit_metrics_events: bool = Field(
        default=False,
        description="Emit PerformanceMetricsEvent during backtest for live progress display",
    )
    event_frequency: int = Field(
        default=100,
        description="Emit metrics event every N bars (only if emit_metrics_events=True)",
    )

    # Risk calculations
    risk_free_rate: float = Field(
        default=0.0,
        description="Annual risk-free rate as decimal (e.g., 0.02 for 2%) for Sharpe/Sortino ratios",
    )

    # Memory management
    max_equity_points: int = Field(
        default=10_000,
        description="Maximum equity curve points to store (sampling applied if exceeded)",
    )

    # Output files
    write_json: bool = Field(
        default=True,
        description="Write performance.json summary report",
    )
    write_parquet: bool = Field(
        default=True,
        description="Write Parquet time-series files (equity_curve, returns, trades, drawdowns)",
    )
    write_csv_timeline: bool = Field(
        default=True,
        description="Write per-strategy CSV timeline files (human-friendly, one file per strategy)",
    )
    include_equity_curve: bool = Field(
        default=True,
        description="Include equity_curve.parquet in output",
    )
    include_returns: bool = Field(
        default=True,
        description="Include returns.parquet in output",
    )
    include_trades: bool = Field(
        default=True,
        description="Include trades.parquet in output",
    )
    include_drawdowns: bool = Field(
        default=True,
        description="Include drawdowns.parquet in output",
    )

    # Console display
    display_final_report: bool = Field(
        default=True,
        description="Display rich-formatted performance report in console at end of backtest",
    )

    # HTML report
    write_html_report: bool = Field(
        default=True,
        description="Generate standalone HTML report with interactive charts",
    )
    report_detail_level: str = Field(
        default="full",
        description="Report detail level: 'summary' (basic metrics), 'standard' (+ risk metrics), 'full' (+ period breakdowns)",
    )

    # Note: Output directory comes from SystemConfig.output.experiments_root
    # Not configurable per backtest - ensures consistent output structure

    # Benchmark (not yet implemented)
    benchmark_symbol: str | None = Field(
        default=None,
        description="Benchmark symbol for comparison (not yet implemented)",
    )


class FeatureConfig(BaseModel):
    """ClickHouse feature store configuration.

    Validated at config-load time so typos or unsupported column names are
    caught before the engine starts, rather than silently connecting to
    unexpected defaults.
    """

    feature_version: str = Field(default="v1", description="Feature set version tag (e.g. 'v1')")
    regime_version: str = Field(default="v1", description="Regime version tag (e.g. 'v1')")
    columns: list[str] | None = Field(
        default=None,
        description=(
            "Feature columns to return (None = all default columns). "
            "When set, every ctx.get_features() call that does not supply an "
            "explicit columns argument will be filtered to this list. "
            "Reduces returned payload and FeatureBarEvent size; the SQL query "
            "still fetches all feature columns and filtering is applied in-process."
        ),
    )
    connect_timeout: int = Field(default=10, ge=1, description="ClickHouse connection timeout in seconds")
    query_timeout: int = Field(default=30, ge=1, description="ClickHouse query timeout in seconds")


class BacktestConfig(BaseModel):
    """Backtest run configuration.

    Contains ONLY per-run parameters:
    - Backtest ID (descriptive identifier for organizing results)
    - Dates, equity (what to test)
    - Data sources with per-source universes
    - Strategy configurations (which strategies to run)
    - Risk policy (Portfolio Manager level)
    - Run-level price basis

    Service configurations (execution, portfolio, logging) come from SystemConfig (system.yaml).

    Price basis contract:
        - `price_basis` is the single run-level contract and defaults to `adjusted`.
        - Both `raw` and `adjusted` are valid runtime values for strategies, execution,
            portfolio accounting, and reporting.

    Example YAML:
        ```yaml
        backtest_id: buy_and_hold  # Descriptive identifier (required)
        start_date: 2020-01-01
        end_date: 2023-12-31
        initial_equity: 100000
        replay_speed: 0.0  # Full speed (default). Use 1.0 for 1 sec/bar

        price_basis: adjusted

        data:
          sources:
            - name: yahoo-us-equity-1d-csv
              universe: [AAPL, MSFT, GOOGL]  # Load these from yahoo
            - name: news-sentiment-feed
              universe: [AAPL]  # News for AAPL only

        strategies:
          - strategy_id: momentum_20  # Registry name
            universe: [AAPL, MSFT]  # Strategy trades subset of loaded symbols
            data_sources: [yahoo-us-equity-1d-csv]  # Which feeds to use
            config:
              lookback: 20
              warmup_bars: 21  # Strategy-specific warmup

        risk_policy:
          name: naive  # Registry name
          config:
            max_pct_position_size: 0.30
        ```
    """

    # Backtest identification
    backtest_id: str = Field(
        ...,
        description="Descriptive identifier for this backtest (used for organizing output directories). "
        "Will be sanitized to filesystem-safe format (lowercase, alphanumeric + underscores).",
    )

    # Backtest parameters
    start_date: datetime = Field(..., description="Backtest start date")
    end_date: datetime = Field(..., description="Backtest end date")
    initial_equity: Decimal = Field(..., description="Starting equity")
    replay_speed: float = Field(
        default=0.0,
        ge=-1.0,
        description="Replay speed for event visualization. "
        "-1.0 = silent (no display), 0.0 = instant display, positive = delay per event in seconds.",
    )
    display_events: list[str] | None = Field(
        default=None,
        description="Events to display during backtest (e.g., ['bar', 'signal', 'fill']). "
        "Use '*' for all events. Omit or empty for silent mode (same as replay_speed=-1.0).",
    )

    # Data, strategies, and risk
    data: DataSelectionConfig = Field(..., description="Data sources with per-source universes")
    strategies: list[StrategyConfigItem] = Field(..., description="Strategy configurations")
    risk_policy: RiskPolicyConfig = Field(..., description="Risk policy (Portfolio Manager level)")

    price_basis: PriceBasis = Field(
        default=PriceBasis.ADJUSTED,
        description="Single run-level price basis contract for strategies, execution, portfolio, and reporting. "
        "Supported values: 'raw' and 'adjusted'.",
    )

    # Reporting (optional)
    reporting: ReportingConfigItem | None = Field(
        default=None,
        description="Performance reporting configuration (optional). "
        "If omitted, no performance metrics will be calculated or displayed.",
    )

    # Feature store (optional)
    feature_config: FeatureConfig | None = Field(
        default=None,
        description="ClickHouse feature store configuration (optional). "
        "If set, FeatureService is constructed and injected into StrategyContext "
        "so strategies can call ctx.get_features(), ctx.get_indicators(), ctx.get_regime(). "
        "Fields: feature_version (str, default 'v1'), regime_version (str, default 'v1'), "
        "columns (list[str] | null — feature columns to fetch, null = all), "
        "connect_timeout (int, default 10), query_timeout (int, default 30).",
    )

    # Remote-runner / job metadata (optional)
    run_id: str | None = Field(
        default=None,
        description="Opaque per-run identifier assigned by the caller. "
        "Used by service-owned database-only executions where run metadata "
        "cannot be derived from a filesystem path.",
    )
    job_group_id: str | None = Field(
        default=None,
        description="Opaque identifier that groups related runs (e.g. a parameter sweep). "
        "Persisted to the operational runs table so sweep results can be aggregated.",
    )
    submission_source: str | None = Field(
        default=None,
        description="System or agent that submitted this run (e.g. 'dashboard', 'cli'). "
        "Persisted to the operational runs table for provenance tracking.",
    )

    # In-sample / out-of-sample split metadata (optional)
    split_pct: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Fraction of the date range used as in-sample data (0.0–1.0). "
        "NULL when no IS/OOS split is applied. Persisted to the operational runs table.",
    )
    split_role: str | None = Field(
        default=None,
        description="Role of this run within an IS/OOS split: 'in_sample' or 'out_of_sample'. "
        "NULL when no split is applied. Persisted to the operational runs table.",
    )

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_adjustment_fields(cls, data: Any) -> Any:
        """Fail fast on the removed adjustment-mode config contract."""
        if not isinstance(data, dict):
            return data

        legacy_keys = [
            key
            for key in ("strategy_adjustment_mode", "portfolio_adjustment_mode", "adjustment_mode")
            if data.get(key) is not None
        ]
        if legacy_keys:
            joined = ", ".join(legacy_keys)
            raise ValueError(
                f"Legacy adjustment-mode fields are no longer supported ({joined}). "
                "Use 'price_basis' with 'raw' or 'adjusted'."
            )

        return data

    @property
    def all_symbols(self) -> set[str]:
        """Get all symbols across all data sources."""
        symbols = set()
        for source in self.data.sources:
            symbols.update(source.universe)
        return symbols

    @property
    def sanitized_backtest_id(self) -> str:
        """Get filesystem-safe version of backtest_id.

        Converts to lowercase and replaces spaces/special chars with underscores.
        Only allows: lowercase letters, numbers, underscores, hyphens.

        Examples:
            "Buy and Hold" -> "buy_and_hold"
            "My Strategy (v2.0)" -> "my_strategy_v2_0"
            "SMA-Crossover Test #1" -> "sma-crossover_test_1"
        """
        # Convert to lowercase
        safe_id = self.backtest_id.lower()
        # Replace spaces and special chars with underscores, keep alphanumeric and hyphens
        safe_id = re.sub(r"[^a-z0-9\-]+", "_", safe_id)
        # Remove leading/trailing underscores
        safe_id = safe_id.strip("_")
        # Collapse multiple underscores
        safe_id = re.sub(r"_+", "_", safe_id)
        return safe_id

    @field_validator("backtest_id")
    @classmethod
    def validate_backtest_id(cls, v: str) -> str:
        """Validate backtest_id is not empty."""
        if not v or not v.strip():
            raise ValueError("backtest_id cannot be empty")
        return v.strip()

    @field_validator("end_date")
    @classmethod
    def validate_dates(cls, v: datetime, info) -> datetime:
        """Validate end_date is after start_date."""
        if "start_date" in info.data and v <= info.data["start_date"]:
            raise ValueError("end_date must be after start_date")
        return v

    @field_validator("data")
    @classmethod
    def validate_single_source(cls, v: DataSelectionConfig) -> DataSelectionConfig:
        """Validate only one data source (multi-source support pending).

        Current limitation: Engine only streams from sources[0], additional sources
        are silently ignored. Enforce single source until multi-source streaming
        is implemented.
        """
        if len(v.sources) > 1:
            raise ValueError(
                f"Multiple data sources not yet supported. "
                f"Found {len(v.sources)} sources: {[s.name for s in v.sources]}. "
                f"Use a single source that provides all required symbols."
            )
        return v

    @field_validator("price_basis", mode="before")
    @classmethod
    def validate_price_basis(cls, v: PriceBasis | str) -> PriceBasis:
        """Validate the single run-level price-basis contract."""
        return PriceBasis.coerce(v)

    @field_validator("strategies")
    @classmethod
    def validate_strategy_universe(cls, v: list[StrategyConfigItem], info) -> list[StrategyConfigItem]:
        """Validate strategy universes are subsets of loaded symbols."""
        if "data" not in info.data:
            return v

        # Get all symbols from all data sources
        all_symbols = set()
        for source in info.data["data"].sources:
            all_symbols.update(source.universe)

        # Validate each strategy
        for strategy in v:
            strategy_symbols = set(strategy.universe)
            if not strategy_symbols.issubset(all_symbols):
                missing = strategy_symbols - all_symbols
                raise ValueError(
                    f"Strategy '{strategy.strategy_id}' universe contains symbols not in data sources: {missing}"
                )

        return v


class ConfigLoadError(Exception):
    """Raised when config loading fails."""

    pass


def load_backtest_config(config_path: str | Path) -> BacktestConfig:
    """
    Load and validate backtest configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Validated BacktestConfig object

    Raises:
        ConfigLoadError: If file not found, invalid YAML, or validation fails

    Example:
        >>> config = load_backtest_config("my_backtest.yaml")
        >>> print(f"Running backtest from {config.start_date} to {config.end_date}")
        >>> print(f"Universe: {config.universe}")
        >>> print(f"Strategies: {[s.strategy_id for s in config.strategies]}")
    """
    path = Path(config_path)

    # Check file exists
    if not path.exists():
        raise ConfigLoadError(f"Config file not found: {path}")

    # Load YAML
    try:
        with path.open() as f:
            raw_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigLoadError(f"Invalid YAML in {path}: {e}") from e

    if not isinstance(raw_config, dict):
        raise ConfigLoadError(f"Config must be a YAML dictionary, got {type(raw_config)}")

    # Validate and construct BacktestConfig
    try:
        config = BacktestConfig(**raw_config)
    except Exception as e:
        raise ConfigLoadError(f"Config validation failed: {e}") from e

    return config

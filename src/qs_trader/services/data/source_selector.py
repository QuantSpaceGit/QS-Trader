"""Data source selector for flexible provider/asset matching."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AssetClass(Enum):
    """Asset class categories."""

    EQUITY = "equity"
    FUTURES = "futures"
    OPTIONS = "options"
    CRYPTO = "crypto"
    FOREX = "forex"
    FIXED_INCOME = "fixed_income"


class DataType(Enum):
    """Type of market data."""

    OHLCV = "ohlcv"
    TRADES = "trades"
    QUOTES = "quotes"
    GREEKS = "greeks"
    FUNDAMENTALS = "fundamentals"


@dataclass
class DataSourceSelector:
    """
    Structured selector for data sources.

    Specify only the criteria that matter for your use case.
    Resolver will find the best matching source.

    Examples:
        # Specific provider
        DataSourceSelector(provider="yahoo", asset_class=AssetClass.EQUITY)

        # Any provider with fallback
        DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            frequency="1d",
            ...
            fallback_providers=["yahoo"]
        )
        )

        # CME futures
        DataSourceSelector(
            asset_class=AssetClass.FUTURES,
            exchange="CME"
        )
    """

    # Core selection criteria
    provider: Optional[str] = None
    asset_class: Optional[AssetClass] = None
    data_type: DataType = DataType.OHLCV
    frequency: Optional[str] = None

    # Optional refinements
    exchange: Optional[str] = None
    region: Optional[str] = None
    adjustment_mode: Optional[str] = None

    # Fallback providers (try in order)
    fallback_providers: list[str] = field(default_factory=list)

    def matches(self, source_config: dict) -> bool:
        """Check if source config matches this selector's criteria."""
        # Only check specified fields (None = don't care)
        if self.provider and source_config.get("provider") != self.provider:
            return False
        if self.asset_class and source_config.get("asset_class") != self.asset_class.value:
            return False
        # Only check data_type if source_config has it (it has a default value)
        if "data_type" in source_config and source_config.get("data_type") != self.data_type.value:
            return False
        if self.frequency and source_config.get("frequency") != self.frequency:
            return False
        if self.exchange and source_config.get("exchange") != self.exchange:
            return False
        if self.region and source_config.get("region") != self.region:
            return False
        if self.adjustment_mode and source_config.get("adjustment_mode") != self.adjustment_mode:
            return False
        return True

    def to_tag(self) -> str:
        """Generate human-readable tag for logging."""
        parts = []
        if self.provider:
            parts.append(self.provider)
        if self.asset_class:
            parts.append(self.asset_class.value)
        if self.data_type != DataType.OHLCV:
            parts.append(self.data_type.value)
        if self.frequency:
            parts.append(self.frequency)
        if self.exchange:
            parts.append(self.exchange)
        return "-".join(parts) if parts else "any"

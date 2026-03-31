"""Configuration for data loading and validation."""

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from qs_trader.services.data.source_selector import DataSourceSelector


class ValidationConfig(BaseModel):
    """OHLC validation configuration."""

    epsilon: float = Field(default=0.0, description="Tolerance for OHLC checks")
    ohlc_policy: str = Field(default="strict_raise", description="Policy for malformed bars")
    close_only_fields: list[str] = Field(default=["close"], description="Fields to trust in close-only mode")


class BarSchemaConfig(BaseModel):
    """Mapping from vendor columns to canonical Bar fields."""

    ts: str = Field(description="Vendor column for timestamp")
    symbol: str = Field(description="Vendor column for symbol")
    open: str = Field(description="Vendor column for open price")
    high: str = Field(description="Vendor column for high price")
    low: str = Field(description="Vendor column for low price")
    close: str = Field(description="Vendor column for close price")
    volume: str = Field(description="Vendor column for volume")


class AdjustmentSchemaConfig(BaseModel):
    """Mapping from vendor columns to AdjustmentEvent fields (optional)."""

    ts: str = Field(description="Vendor column for event timestamp")
    symbol: str = Field(description="Vendor column for symbol")
    event_type: str = Field(description="Vendor column for adjustment type")
    px_factor: str = Field(description="Vendor column for price factor")
    vol_factor: str = Field(description="Vendor column for volume factor")
    metadata_fields: list[str] = Field(default_factory=list, description="Additional fields to capture")


class DataConfig(BaseModel):
    """Data loading and processing configuration."""

    mode: str = Field(
        default="adjusted",
        description="Data adjustment mode (adjusted|unadjusted|split_adjusted)",
    )
    frequency: str = Field(default="1d", description="Bar frequency (1m|5m|15m|1h|1d)")
    timezone: str = Field(default="America/New_York", description="Timezone for timestamps")
    source_selector: DataSourceSelector = Field(
        description="Data source selector - structured selection by provider, asset class, etc."
    )
    validation: ValidationConfig = Field(default_factory=ValidationConfig, description="Validation rules")
    bar_schema: BarSchemaConfig = Field(description="Vendor schema → Bar mapping")
    adjustment_schema: Optional[AdjustmentSchemaConfig] = Field(
        default=None, description="Vendor schema → AdjustmentEvent mapping (optional)"
    )

    @field_validator("source_selector")
    @classmethod
    def validate_selector(cls, v: DataSourceSelector) -> DataSourceSelector:
        """Ensure at least one criterion specified in selector."""
        if not any([v.provider, v.asset_class, v.exchange, v.frequency]):
            raise ValueError(
                "DataSourceSelector must specify at least one criterion (provider, asset_class, exchange, or frequency)"
            )
        return v

    @classmethod
    def from_yaml(cls, path: Path) -> "DataConfig":
        """Load configuration from YAML file."""
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data.get("data", {}))

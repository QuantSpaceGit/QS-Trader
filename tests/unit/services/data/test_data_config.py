"""Tests for data configuration."""

import pytest

from qs_trader.services.data.config import AdjustmentSchemaConfig, BarSchemaConfig, DataConfig, ValidationConfig
from qs_trader.services.data.source_selector import AssetClass, DataSourceSelector


def test_validation_config_defaults():
    """ValidationConfig should have sensible defaults."""
    config = ValidationConfig()
    assert config.epsilon == 0.0
    assert config.ohlc_policy == "strict_raise"
    assert config.close_only_fields == ["close"]


def test_bar_schema_config_creation():
    """BarSchemaConfig should map vendor columns to Bar fields."""
    schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="MarketHoursVolume",
    )
    assert schema.ts == "TradeDate"
    assert schema.symbol == "Ticker"
    assert schema.volume == "MarketHoursVolume"


def test_adjustment_schema_config_creation():
    """AdjustmentSchemaConfig should map vendor columns to AdjustmentEvent fields."""
    schema = AdjustmentSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        event_type="AdjustmentReason",
        px_factor="CumulativePriceFactor",
        vol_factor="CumulativeVolumeFactor",
        metadata_fields=["AdjustmentFactor"],
    )
    assert schema.ts == "TradeDate"
    assert schema.event_type == "AdjustmentReason"
    assert schema.metadata_fields == ["AdjustmentFactor"]


def test_data_config_defaults():
    """DataConfig should have sensible defaults."""
    # Need bar_schema and source_selector as required fields
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(provider="test_provider", asset_class=AssetClass.EQUITY)
    config = DataConfig(bar_schema=bar_schema, source_selector=selector)

    assert config.mode == "adjusted"
    assert config.frequency == "1d"
    assert config.timezone == "America/New_York"
    assert config.validation.ohlc_policy == "strict_raise"
    assert config.source_selector.provider == "test_provider"
    assert config.source_selector.asset_class == AssetClass.EQUITY


def test_data_config_with_adjustment_schema():
    """DataConfig should accept optional adjustment_schema."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(provider="test_provider", asset_class=AssetClass.EQUITY)
    adj_schema = AdjustmentSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        event_type="AdjustmentReason",
        px_factor="CumulativePriceFactor",
        vol_factor="CumulativeVolumeFactor",
    )
    config = DataConfig(bar_schema=bar_schema, source_selector=selector, adjustment_schema=adj_schema)

    assert config.adjustment_schema is not None
    assert config.adjustment_schema.event_type == "AdjustmentReason"


def test_data_config_selector_validation_missing_criteria():
    """DataConfig should reject selector with no criteria specified."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    # Empty selector (no criteria)
    selector = DataSourceSelector()

    with pytest.raises(ValueError, match="must specify at least one criterion"):
        DataConfig(bar_schema=bar_schema, source_selector=selector)


def test_data_config_selector_with_provider():
    """DataConfig should accept selector with provider specified."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(provider="test_provider")
    config = DataConfig(bar_schema=bar_schema, source_selector=selector)

    assert config.source_selector.provider == "test_provider"


def test_data_config_selector_with_asset_class():
    """DataConfig should accept selector with asset class specified."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(asset_class=AssetClass.FUTURES)
    config = DataConfig(bar_schema=bar_schema, source_selector=selector)

    assert config.source_selector.asset_class == AssetClass.FUTURES


def test_data_config_selector_with_exchange():
    """DataConfig should accept selector with exchange specified."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(exchange="CME")
    config = DataConfig(bar_schema=bar_schema, source_selector=selector)

    assert config.source_selector.exchange == "CME"


def test_data_config_selector_with_frequency():
    """DataConfig should accept selector with frequency specified."""
    bar_schema = BarSchemaConfig(
        ts="TradeDate",
        symbol="Ticker",
        open="Open",
        high="High",
        low="Low",
        close="Close",
        volume="Volume",
    )
    selector = DataSourceSelector(frequency="1h")
    config = DataConfig(bar_schema=bar_schema, source_selector=selector)

    assert config.source_selector.frequency == "1h"

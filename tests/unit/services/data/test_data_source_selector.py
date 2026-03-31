"""Tests for data source selector."""

from qs_trader.services.data.source_selector import AssetClass, DataSourceSelector, DataType


class TestAssetClass:
    """Test AssetClass enum."""

    def test_all_asset_classes(self):
        """Test all asset class values."""
        assert AssetClass.EQUITY.value == "equity"
        assert AssetClass.FUTURES.value == "futures"
        assert AssetClass.OPTIONS.value == "options"
        assert AssetClass.CRYPTO.value == "crypto"
        assert AssetClass.FOREX.value == "forex"
        assert AssetClass.FIXED_INCOME.value == "fixed_income"


class TestDataType:
    """Test DataType enum."""

    def test_all_data_types(self):
        """Test all data type values."""
        assert DataType.OHLCV.value == "ohlcv"
        assert DataType.TRADES.value == "trades"
        assert DataType.QUOTES.value == "quotes"
        assert DataType.GREEKS.value == "greeks"
        assert DataType.FUNDAMENTALS.value == "fundamentals"


class TestDataSourceSelector:
    """Test DataSourceSelector class."""

    def test_default_construction(self):
        """Test default selector construction."""
        selector = DataSourceSelector()
        assert selector.provider is None
        assert selector.asset_class is None
        assert selector.data_type == DataType.OHLCV
        assert selector.frequency is None
        assert selector.exchange is None
        assert selector.region is None
        assert selector.adjustment_mode is None
        assert selector.fallback_providers == []

    def test_provider_only(self):
        """Test selector with only provider specified."""
        selector = DataSourceSelector(provider="test_provider")
        assert selector.provider == "test_provider"
        assert selector.asset_class is None

    def test_asset_class_only(self):
        """Test selector with only asset class specified."""
        selector = DataSourceSelector(asset_class=AssetClass.EQUITY)
        assert selector.asset_class == AssetClass.EQUITY
        assert selector.provider is None

    def test_full_specification(self):
        """Test fully specified selector."""
        selector = DataSourceSelector(
            provider="test_provider",
            asset_class=AssetClass.EQUITY,
            data_type=DataType.OHLCV,
            frequency="1d",
            exchange="NYSE",
            region="US",
            adjustment_mode="adjusted",
            fallback_providers=["test_provider", "polygon"],
        )
        assert selector.provider == "test_provider"
        assert selector.asset_class == AssetClass.EQUITY
        assert selector.data_type == DataType.OHLCV
        assert selector.frequency == "1d"
        assert selector.exchange == "NYSE"
        assert selector.region == "US"
        assert selector.adjustment_mode == "adjusted"
        assert selector.fallback_providers == ["test_provider", "polygon"]

    def test_matches_exact(self):
        """Test matching with exact criteria."""
        selector = DataSourceSelector(
            provider="test_provider",
            asset_class=AssetClass.EQUITY,
            frequency="1d",
        )

        source_config = {
            "provider": "test_provider",
            "asset_class": "equity",
            "data_type": "ohlcv",
            "frequency": "1d",
        }

        assert selector.matches(source_config) is True

    def test_matches_provider_only(self):
        """Test matching with only provider specified."""
        selector = DataSourceSelector(provider="test_provider")

        # Should match any source with test_provider
        source_config = {
            "provider": "test_provider",
            "asset_class": "equity",
            "frequency": "1d",
        }
        assert selector.matches(source_config) is True

        # Should not match different provider
        source_config["provider"] = "polygon"
        assert selector.matches(source_config) is False

    def test_matches_asset_class_only(self):
        """Test matching with only asset class specified."""
        selector = DataSourceSelector(asset_class=AssetClass.FUTURES)

        # Should match any source with futures
        source_config = {
            "provider": "cme",
            "asset_class": "futures",
            "frequency": "1d",
        }
        assert selector.matches(source_config) is True

        # Should not match different asset class
        source_config["asset_class"] = "equity"
        assert selector.matches(source_config) is False

    def test_matches_none_criteria(self):
        """Test matching with no criteria (matches everything)."""
        selector = DataSourceSelector()

        # Should match any source
        source_config = {
            "provider": "any",
            "asset_class": "anything",
        }
        assert selector.matches(source_config) is True

    def test_matches_multiple_criteria(self):
        """Test matching with multiple criteria."""
        selector = DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            frequency="1d",
            region="US",
        )

        # All criteria match
        source_config = {
            "provider": "test_provider",
            "asset_class": "equity",
            "frequency": "1d",
            "region": "US",
        }
        assert selector.matches(source_config) is True

        # One criterion doesn't match (frequency)
        source_config["frequency"] = "1h"
        assert selector.matches(source_config) is False

    def test_matches_with_exchange(self):
        """Test matching with exchange specified."""
        selector = DataSourceSelector(
            asset_class=AssetClass.FUTURES,
            exchange="CME",
        )

        source_config = {
            "provider": "cmeDataMine",
            "asset_class": "futures",
            "exchange": "CME",
        }
        assert selector.matches(source_config) is True

        source_config["exchange"] = "ICE"
        assert selector.matches(source_config) is False

    def test_matches_with_adjustment_mode(self):
        """Test matching with adjustment mode specified."""
        selector = DataSourceSelector(
            provider="test_provider",
            adjustment_mode="adjusted",
        )

        source_config = {
            "provider": "test_provider",
            "asset_class": "equity",
            "adjustment_mode": "adjusted",
        }
        assert selector.matches(source_config) is True

        source_config["adjustment_mode"] = "unadjusted"
        assert selector.matches(source_config) is False

    def test_matches_data_type(self):
        """Test matching with data type specified."""
        selector = DataSourceSelector(
            asset_class=AssetClass.OPTIONS,
            data_type=DataType.GREEKS,
        )

        source_config = {
            "provider": "optiontrade",
            "asset_class": "options",
            "data_type": "greeks",
        }
        assert selector.matches(source_config) is True

        source_config["data_type"] = "ohlcv"
        assert selector.matches(source_config) is False

    def test_to_tag_empty(self):
        """Test tag generation with no criteria."""
        selector = DataSourceSelector()
        assert selector.to_tag() == "any"

    def test_to_tag_provider_only(self):
        """Test tag generation with provider only."""
        selector = DataSourceSelector(provider="test_provider")
        assert selector.to_tag() == "test_provider"

    def test_to_tag_asset_class_only(self):
        """Test tag generation with asset class only."""
        selector = DataSourceSelector(asset_class=AssetClass.EQUITY)
        assert selector.to_tag() == "equity"

    def test_to_tag_provider_and_asset_class(self):
        """Test tag generation with provider and asset class."""
        selector = DataSourceSelector(
            provider="test_provider",
            asset_class=AssetClass.EQUITY,
        )
        assert selector.to_tag() == "test_provider-equity"

    def test_to_tag_full(self):
        """Test tag generation with all criteria."""
        selector = DataSourceSelector(
            provider="cme",
            asset_class=AssetClass.FUTURES,
            data_type=DataType.TRADES,
            frequency="1h",
            exchange="CME",
        )
        assert selector.to_tag() == "cme-futures-trades-1h-CME"

    def test_to_tag_ohlcv_not_included(self):
        """Test that OHLCV (default) is not included in tag."""
        selector = DataSourceSelector(
            provider="test_provider",
            asset_class=AssetClass.EQUITY,
            data_type=DataType.OHLCV,  # Default, should not appear
            frequency="1d",
        )
        assert selector.to_tag() == "test_provider-equity-1d"

    def test_fallback_providers(self):
        """Test fallback providers list."""
        selector = DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            fallback_providers=["test_provider", "test_provider", "polygon"],
        )
        assert selector.fallback_providers == ["test_provider", "test_provider", "polygon"]

    def test_fallback_providers_empty_default(self):
        """Test that fallback providers defaults to empty list."""
        selector = DataSourceSelector(provider="test_provider")
        assert selector.fallback_providers == []
        # Ensure it's mutable and not shared
        selector.fallback_providers.append("backup")
        selector2 = DataSourceSelector(provider="test_provider")
        assert selector2.fallback_providers == []

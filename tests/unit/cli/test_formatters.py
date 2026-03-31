"""Unit tests for CLI UI formatters.

Tests the Rich table formatters and row builders.
"""

from rich.table import Table

from qs_trader.cli.ui.formatters import (
    add_bar_data,
    add_cache_info_row,
    add_update_result_row,
    create_bar_table,
    create_cache_info_table,
    create_update_summary_table,
)


class TestBarTable:
    """Test bar table creation and population."""

    def test_create_bar_table(self):
        """Test creating a bar table."""
        table = create_bar_table("AAPL", 1, 10)

        assert isinstance(table, Table)
        assert table.title is not None

    def test_add_bar_data(self):
        """Test adding bar data to table."""
        table = create_bar_table("AAPL", 1, 10)

        bar_data = {
            "date": "2020-01-02",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 104.0,
            "volume": 1000000,
        }

        # Should not raise
        add_bar_data(table, bar_data)

        # Table should have columns after adding data
        assert len(table.columns) == 2

    def test_add_bar_data_with_dividend(self):
        """Test adding bar data with dividend."""
        table = create_bar_table("AAPL", 1, 10)

        bar_data = {
            "date": "2020-01-02",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 104.0,
            "volume": 1000000,
            "dividend": 0.82,
        }

        add_bar_data(table, bar_data)

        # Should have dividend row
        assert len(table.columns) == 2


class TestUpdateSummaryTable:
    """Test update summary table creation and population."""

    def test_create_update_summary_table(self):
        """Test creating an empty update summary table."""
        table = create_update_summary_table()

        assert isinstance(table, Table)
        assert table.title == "Update Summary"
        # Should have 5 columns
        assert len(table.columns) == 5

    def test_add_update_result_row_success(self):
        """Test adding successful update result."""
        table = create_update_summary_table()

        add_update_result_row(
            table=table,
            symbol="AAPL",
            success=True,
            bars_added=10,
            start_date="2020-01-01",
            end_date="2020-12-31",
            row_count="252",
        )

        assert len(table.rows) == 1

    def test_add_update_result_row_failure(self):
        """Test adding failed update result."""
        table = create_update_summary_table()

        add_update_result_row(
            table=table,
            symbol="BADSTOCK",
            success=False,
            bars_added=0,
            start_date=None,
            end_date=None,
            row_count=None,
            error="Symbol not found",
        )

        assert len(table.rows) == 1

    def test_add_update_result_row_no_change(self):
        """Test adding no-change update result."""
        table = create_update_summary_table()

        add_update_result_row(
            table=table,
            symbol="AAPL",
            success=True,
            bars_added=0,
            start_date="2020-01-01",
            end_date="2020-12-31",
            row_count="252",
        )

        assert len(table.rows) == 1


class TestCacheInfoTable:
    """Test cache info table creation and population."""

    def test_create_cache_info_table(self):
        """Test creating an empty cache info table."""
        table = create_cache_info_table()

        assert isinstance(table, Table)
        assert table.title == "Cached Symbols"
        # Should have 5 columns
        assert len(table.columns) == 5

    def test_add_cache_info_row_with_data(self):
        """Test adding cache info for symbol with data."""
        table = create_cache_info_table()

        add_cache_info_row(
            table=table,
            symbol="AAPL",
            start_date="2020-01-01",
            end_date="2020-12-31",
            row_count="252",
            last_update="2024-01-01T12:00:00",
        )

        assert len(table.rows) == 1

    def test_add_cache_info_row_no_data(self):
        """Test adding cache info for symbol with no data."""
        table = create_cache_info_table()

        add_cache_info_row(
            table=table,
            symbol="NEWSTOCK",
            start_date="N/A",
            end_date="N/A",
            row_count="0",
            last_update="N/A",
        )

        assert len(table.rows) == 1

    def test_add_multiple_cache_info_rows(self):
        """Test adding multiple cache info rows."""
        table = create_cache_info_table()

        symbols = ["AAPL", "MSFT", "GOOGL"]
        for symbol in symbols:
            add_cache_info_row(
                table=table,
                symbol=symbol,
                start_date="2020-01-01",
                end_date="2020-12-31",
                row_count="252",
                last_update="2024-01-01T12:00:00",
            )

        assert len(table.rows) == 3


class TestTableFormatting:
    """Test table formatting consistency."""

    def test_all_tables_use_rich_table(self):
        """Test that all formatters return Rich Table objects."""
        bar_table = create_bar_table("AAPL", 1, 10)
        update_table = create_update_summary_table()
        cache_table = create_cache_info_table()

        assert isinstance(bar_table, Table)
        assert isinstance(update_table, Table)
        assert isinstance(cache_table, Table)

    def test_tables_have_titles(self):
        """Test that all tables have titles."""
        update_table = create_update_summary_table()
        cache_table = create_cache_info_table()

        assert update_table.title is not None
        assert cache_table.title is not None

    def test_tables_have_columns(self):
        """Test that all tables have columns defined."""
        update_table = create_update_summary_table()
        cache_table = create_cache_info_table()

        # Bar table has no columns until data is added
        assert len(update_table.columns) > 0
        assert len(cache_table.columns) > 0

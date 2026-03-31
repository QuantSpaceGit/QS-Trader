"""Test configuration and fixtures for portfolio service tests."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.portfolio.models import PortfolioConfig


@pytest.fixture
def basic_config():
    """Basic portfolio configuration for tests."""
    return PortfolioConfig(
        initial_cash=Decimal("100000.00"),
        default_commission_per_share=Decimal("0.01"),
        default_borrow_rate_apr=Decimal("0.05"),
        margin_rate_apr=Decimal("0.07"),
    )


@pytest.fixture
def timestamp():
    """Standard timestamp for tests."""
    return datetime(2020, 1, 2, 9, 30, 0)

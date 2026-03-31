"""Unit tests for risk policy loaders.

Tests loader functions in libraries/risk/loaders.py that parse YAML risk policies.

Coverage focus:
- _parse_budgets: Parses strategy budget allocations from YAML
- Budget weight validation (sum ≤ 1.0)
- Default budget fallback behavior
"""

from pathlib import Path

import pytest

from qs_trader.libraries.risk.loaders import _parse_budgets


class TestParseBudgets:
    """Test suite for _parse_budgets function."""

    def test_no_budgets_section_returns_default_budget(self):
        """Test that missing budgets section creates default 95% budget."""
        # Arrange
        policy = {"sizing": {}}  # No budgets section
        source_path = Path("test.yaml")

        # Act
        budgets = _parse_budgets(policy, source_path)

        # Assert
        assert len(budgets) == 1
        assert budgets[0].strategy_id == "default"
        assert budgets[0].capital_weight == 0.95

    def test_single_strategy_budget_parsed_correctly(self):
        """Test parsing a single strategy budget allocation."""
        # Arrange
        policy = {"budgets": [{"strategy_id": "sma_crossover", "capital_weight": 0.5}]}
        source_path = Path("test.yaml")

        # Act
        budgets = _parse_budgets(policy, source_path)

        # Assert
        assert len(budgets) == 1
        assert budgets[0].strategy_id == "sma_crossover"
        assert budgets[0].capital_weight == 0.5

    def test_multiple_strategy_budgets_parsed_correctly(self):
        """Test parsing multiple strategy budget allocations."""
        # Arrange
        policy = {
            "budgets": [
                {"strategy_id": "sma_crossover", "capital_weight": 0.30},
                {"strategy_id": "momentum", "capital_weight": 0.30},
                {"strategy_id": "default", "capital_weight": 0.35},
            ]
        }
        source_path = Path("test.yaml")

        # Act
        budgets = _parse_budgets(policy, source_path)

        # Assert
        assert len(budgets) == 3
        assert budgets[0].strategy_id == "sma_crossover"
        assert budgets[0].capital_weight == 0.30
        assert budgets[1].strategy_id == "momentum"
        assert budgets[1].capital_weight == 0.30
        assert budgets[2].strategy_id == "default"
        assert budgets[2].capital_weight == 0.35

    def test_budget_weights_summing_to_one_is_valid(self):
        """Test that budget weights summing to exactly 1.0 is valid."""
        # Arrange
        policy = {
            "budgets": [
                {"strategy_id": "strategy_a", "capital_weight": 0.5},
                {"strategy_id": "strategy_b", "capital_weight": 0.5},
            ]
        }
        source_path = Path("test.yaml")

        # Act
        budgets = _parse_budgets(policy, source_path)

        # Assert
        assert len(budgets) == 2
        total = sum(b.capital_weight for b in budgets)
        assert total == 1.0

    def test_budget_weights_less_than_one_is_valid(self):
        """Test that budget weights summing to < 1.0 is valid (leaves cash reserve)."""
        # Arrange
        policy = {
            "budgets": [
                {"strategy_id": "strategy_a", "capital_weight": 0.4},
                {"strategy_id": "strategy_b", "capital_weight": 0.3},
            ]
        }
        source_path = Path("test.yaml")

        # Act
        budgets = _parse_budgets(policy, source_path)

        # Assert
        assert len(budgets) == 2
        total = sum(b.capital_weight for b in budgets)
        assert total == 0.7  # 30% unallocated

    def test_budget_weights_exceeding_one_raises_error(self):
        """Test that budget weights summing to > 1.0 raises ValueError."""
        # Arrange
        policy = {
            "budgets": [
                {"strategy_id": "strategy_a", "capital_weight": 0.6},
                {"strategy_id": "strategy_b", "capital_weight": 0.6},
            ]
        }
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="budget weights sum to 1.20, must be ≤ 1.0"):
            _parse_budgets(policy, source_path)

    def test_missing_strategy_id_raises_error(self):
        """Test that budget without strategy_id raises ValueError."""
        # Arrange
        policy = {
            "budgets": [
                {"capital_weight": 0.5}  # Missing strategy_id
            ]
        }
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="missing 'strategy_id'"):
            _parse_budgets(policy, source_path)

    def test_missing_capital_weight_raises_error(self):
        """Test that budget without capital_weight raises ValueError."""
        # Arrange
        policy = {
            "budgets": [
                {"strategy_id": "test"}  # Missing capital_weight
            ]
        }
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="missing 'capital_weight'"):
            _parse_budgets(policy, source_path)

    def test_non_numeric_weight_raises_error(self):
        """Test that non-numeric capital_weight raises ValueError."""
        # Arrange
        policy = {"budgets": [{"strategy_id": "test", "capital_weight": "invalid"}]}
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="must be numeric"):
            _parse_budgets(policy, source_path)

    def test_negative_weight_raises_error(self):
        """Test that negative capital_weight raises ValueError."""
        # Arrange
        policy = {"budgets": [{"strategy_id": "test", "capital_weight": -0.1}]}
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="must be in \\[0, 1\\]"):
            _parse_budgets(policy, source_path)

    def test_weight_exceeding_one_raises_error(self):
        """Test that capital_weight > 1.0 raises ValueError."""
        # Arrange
        policy = {"budgets": [{"strategy_id": "test", "capital_weight": 1.5}]}
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="must be in \\[0, 1\\]"):
            _parse_budgets(policy, source_path)

    def test_budgets_not_list_raises_error(self):
        """Test that non-list budgets section raises ValueError."""
        # Arrange
        policy = {
            "budgets": {"strategy_id": "test", "capital_weight": 0.5}  # Dict instead of list
        }
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="must be a list"):
            _parse_budgets(policy, source_path)

    def test_budget_entry_not_dict_raises_error(self):
        """Test that non-dict budget entry raises ValueError."""
        # Arrange
        policy = {
            "budgets": ["invalid_entry"]  # String instead of dict
        }
        source_path = Path("test.yaml")

        # Act & Assert
        with pytest.raises(ValueError, match="each budget must be a dict"):
            _parse_budgets(policy, source_path)

"""Unit tests for the filter module."""

import pandas as pd
import pytest

from quick_metric.filter import evaluate_condition


class TestEvaluateCondition:
    """Test evaluate_condition function with focused, single-assertion tests."""

    @pytest.fixture
    def sample_data(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "category": ["A", "B", "A", "C"],
                "value": [10, 20, 30, 40],
                "status": ["active", "inactive", "active", "pending"],
            }
        )

    @pytest.mark.parametrize(
        "column,condition,expected",
        [
            ("category", "A", [True, False, True, False]),
            ("category", "X", [False, False, False, False]),
            ("category", ["A"], [True, False, True, False]),
            ("category", ["A", "B"], [True, True, True, False]),
            ("status", "active", [True, False, True, False]),
            ("status", "nonexistent", [False, False, False, False]),
        ],
    )
    def test_equality_and_membership_conditions(
        self, sample_data, column, condition, expected
    ):
        """Test equality and list membership conditions."""
        result = evaluate_condition(sample_data, column, condition)
        expected_series = pd.Series(expected)
        pd.testing.assert_series_equal(result, expected_series, check_names=False)

    def test_not_condition_simple(self, sample_data):
        """Test NOT condition with simple value."""
        result = evaluate_condition(sample_data, "status", {"not": "active"})
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    @pytest.mark.parametrize(
        "operator,value,expected",
        [
            ("greater than", 25, [False, False, True, True]),
            ("less than", 25, [True, True, False, False]),
            ("greater than equal", 20, [False, True, True, True]),
            ("less than equal", 20, [True, True, False, False]),
        ],
    )
    def test_comparison_operators(self, sample_data, operator, value, expected):
        """Test various comparison operators."""
        result = evaluate_condition(sample_data, "value", {operator: value})
        expected_series = pd.Series(expected)
        pd.testing.assert_series_equal(result, expected_series, check_names=False)

    def test_nonexistent_column_returns_all_false(self, sample_data):
        """Test that non-existent column returns all False values."""
        result = evaluate_condition(sample_data, "nonexistent", "any_value")
        expected = pd.Series([False, False, False, False])
        pd.testing.assert_series_equal(result, expected)

    def test_in_condition(self, sample_data):
        """Test 'in' condition operator."""
        result = evaluate_condition(sample_data, "category", {"in": ["A", "C"]})
        expected = pd.Series([True, False, True, True])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_not_in_condition(self, sample_data):
        """Test 'not in' condition operator."""
        result = evaluate_condition(sample_data, "category", {"not in": ["A", "C"]})
        expected = pd.Series([False, True, False, False])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_is_condition(self, sample_data):
        """Test 'is' condition operator."""
        result = evaluate_condition(sample_data, "value", {"is": 20})
        expected = pd.Series([False, True, False, False])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_empty_dataframe(self):
        """Test evaluate_condition with empty DataFrame."""
        empty_df = pd.DataFrame({"col": []})
        result = evaluate_condition(empty_df, "col", "value")
        expected = pd.Series([], dtype=bool, name="col")
        pd.testing.assert_series_equal(result, expected)

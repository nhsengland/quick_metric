"""Unit tests for the filter module."""

import pandas as pd
import pytest

from quick_metric._filter import evaluate_condition, recursive_filter


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
        ("column", "condition", "expected"),
        [
            ("category", "A", [True, False, True, False]),
            ("category", "X", [False, False, False, False]),
            ("category", ["A"], [True, False, True, False]),
            ("category", ["A", "B"], [True, True, True, False]),
            ("status", "active", [True, False, True, False]),
            ("status", "nonexistent", [False, False, False, False]),
        ],
    )
    def test_equality_and_membership_conditions(self, sample_data, column, condition, expected):
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
        ("operator", "value", "expected"),
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


class TestRecursiveFilter:
    """Test recursive_filter function for complex logical operations."""

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

    def test_or_with_dictionary_format_nested_logical(self, sample_data):
        """Test OR condition with dictionary format containing nested logical operators."""

        # Test nested 'and' within 'or' dictionary format
        filters = {
            "or": {"and": {"category": "A", "value": {"greater than": 25}}, "status": "pending"}
        }

        result = recursive_filter(sample_data, filters)
        # Should match: (category=A AND value>25) OR status=pending
        # Row 0: A, 10, active -> False AND False = False, OR False = False
        # Row 1: B, 20, inactive -> False AND False = False, OR False = False
        # Row 2: A, 30, active -> True AND True = True, OR False = True
        # Row 3: C, 40, pending -> False AND True = False, OR True = True
        expected = pd.Series([False, False, True, True])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_or_dictionary_format_with_none_condition_result(self, sample_data):
        """Test OR dictionary format where evaluate_condition returns None."""

        # Test with a condition that might return None (nonexistent column)
        filters = {"or": {"nonexistent_column": "some_value", "category": "A"}}

        result = recursive_filter(sample_data, filters)
        # Should match where category=A (nonexistent_column condition returns None/False)
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(result, expected, check_names=False)

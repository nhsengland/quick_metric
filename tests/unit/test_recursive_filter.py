"""Unit tests for the recursive_filter function."""

import pandas as pd
import pytest
from unittest.mock import patch

from quick_metric._filter import recursive_filter, evaluate_condition


class TestRecursiveFilter:
    """Test recursive_filter function with mocked dependencies."""

    @pytest.fixture
    def sample_data(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "type": ["A", "B", "A", "B"],
                "value": [10, 20, 30, 40],
                "active": [True, False, True, False],
            }
        )

    @patch("quick_metric._filter.evaluate_condition")
    def test_and_condition_calls_evaluate_condition(self, mock_evaluate, sample_data):
        """Test that AND condition calls evaluate_condition for each key."""
        # Setup mock to return specific boolean series
        mock_evaluate.side_effect = [
            pd.Series([True, False, True, False]),  # type == 'A'
            pd.Series([True, True, False, False]),  # value > 15
        ]

        filters = {"and": {"type": "A", "value": {"greater than": 15}}}

        result = recursive_filter(sample_data, filters)

        # Should call evaluate_condition twice
        assert mock_evaluate.call_count == 2

        # Result should be AND of the two conditions
        expected = pd.Series([True, False, False, False])
        pd.testing.assert_series_equal(result, expected)

    @patch("quick_metric._filter.evaluate_condition")
    def test_or_condition_calls_evaluate_condition(self, mock_evaluate, sample_data):
        """Test that OR condition calls evaluate_condition for each key."""
        mock_evaluate.side_effect = [
            pd.Series([True, False, False, False]),  # type == 'A'
            pd.Series([False, False, False, True]),  # value > 35
        ]

        filters = {"or": {"type": "A", "value": {"greater than": 35}}}

        result = recursive_filter(sample_data, filters)

        assert mock_evaluate.call_count == 2
        expected = pd.Series([True, False, False, True])
        pd.testing.assert_series_equal(result, expected)

    def test_not_condition_inverts_result(self, sample_data):
        """Test that NOT condition properly inverts the result."""
        filters = {"not": {"type": "A"}}

        result = recursive_filter(sample_data, filters)
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_simple_condition_delegates_to_evaluate_condition(self, sample_data):
        """Test that simple condition delegates to evaluate_condition."""
        filters = {"type": "A"}

        result = recursive_filter(sample_data, filters)
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_nested_and_or_conditions(self, sample_data):
        """Test nested AND/OR conditions work correctly."""
        filters = {
            "and": {"or": {"type": "A", "value": {"greater than": 35}}, "active": True}
        }

        result = recursive_filter(sample_data, filters)
        # Should match rows where (type='A' OR value>35) AND active=True
        # Row 0: type='A' AND active=True -> True
        # Row 2: type='A' AND active=True -> True
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(result, expected)

    @pytest.mark.parametrize(
        "condition_type,filters,expected",
        [
            ("and", {"and": {}}, [True, True, True, True]),
            ("or", {"or": {}}, [False, False, False, False]),
        ],
    )
    def test_empty_conditions_return_expected_defaults(
        self, sample_data, condition_type, filters, expected
    ):
        """Test that empty conditions return expected default values."""
        result = recursive_filter(sample_data, filters)
        expected_series = pd.Series(expected)
        pd.testing.assert_series_equal(result, expected_series, check_names=False)

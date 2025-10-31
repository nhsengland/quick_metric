"""Unit tests for recursive_filter function."""

import pandas as pd
import pytest

from quick_metric._filter import recursive_filter


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

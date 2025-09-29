"""Test the new generate_metrics function."""

import pandas as pd
import pytest

from quick_metric import generate_metrics, metric_method


@metric_method
def count_records(data):
    """Count the number of records."""
    return len(data)


@metric_method
def sum_values(data):
    """Sum values in the 'value' column."""
    return data['value'].sum() if 'value' in data.columns else 0


class TestGenerateMetrics:
    """Test cases for the generate_metrics function."""

    def test_generate_metrics_with_dict_config(self):
        """Test generate_metrics with dictionary configuration."""
        # Create test data
        data = pd.DataFrame({
            'category': ['A', 'B', 'A', 'C', 'B', 'A'],
            'value': [10, 20, 15, 30, 25, 5]
        })

        # Test with dictionary configuration
        config = {
            'category_a_metrics': {
                'method': ['count_records', 'sum_values'],
                'filter': {'category': 'A'}
            },
            'category_b_metrics': {
                'method': ['count_records', 'sum_values'],
                'filter': {'category': 'B'}
            }
        }

        # Run the generate_metrics function
        results = generate_metrics(data, config)

        # Verify results
        assert 'category_a_metrics' in results
        assert 'category_b_metrics' in results
        
        # Check category A metrics (3 records with values 10, 15, 5)
        assert results['category_a_metrics']['count_records'] == 3
        assert results['category_a_metrics']['sum_values'] == 30
        
        # Check category B metrics (2 records with values 20, 25)
        assert results['category_b_metrics']['count_records'] == 2
        assert results['category_b_metrics']['sum_values'] == 45

    def test_generate_metrics_with_empty_filter(self):
        """Test generate_metrics with empty filter (all data)."""
        data = pd.DataFrame({
            'category': ['A', 'B'],
            'value': [10, 20]
        })

        config = {
            'all_records': {
                'method': ['count_records'],
                'filter': {}
            }
        }

        results = generate_metrics(data, config)
        assert results['all_records']['count_records'] == 2

    def test_generate_metrics_invalid_config_type(self):
        """Test generate_metrics with invalid config type."""
        data = pd.DataFrame({'col': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Config must be a pathlib.Path"):
            generate_metrics(data, 123)  # type: ignore

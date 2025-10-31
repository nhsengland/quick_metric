"""
End-to-end performance tests for split functionality with large datasets.

These tests ensure that the split_by feature performs well with:
- Large datasets (millions of rows)
- Many split groups (hundreds/thousands)
- Multiple split dimensions
- Large result sets
"""

from pathlib import Path
import pickle
import sys
import time

import numpy as np
import pandas as pd
import pytest

from quick_metric.core import interpret_metric_instructions
from quick_metric.registry import metric_method
from quick_metric.results import DataFrameResult, SeriesResult

# Cache directory for test data
CACHE_DIR = Path(__file__).parent / ".test_data_cache"
CACHE_DIR.mkdir(exist_ok=True)


def _generate_and_cache_large_dataset():
    """Generate large dataset and cache it to pickle file."""
    cache_file = CACHE_DIR / "large_dataset_1m.pkl"

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    # Generate 1M rows
    np.random.seed(42)
    n_rows = 1_000_000

    data = pd.DataFrame(
        {
            "region": np.random.choice(["R" + str(i) for i in range(10)], n_rows),
            "site": np.random.choice(["S" + str(i) for i in range(50)], n_rows),
            "category": np.random.choice(["A", "B", "C", "D", "E"], n_rows),
            "subcategory": np.random.choice(["X", "Y", "Z"], n_rows),
            "value": np.random.randint(1, 1000, n_rows),
            "amount": np.random.uniform(10, 10000, n_rows),
        }
    )

    # Cache to pickle
    with open(cache_file, "wb") as f:
        pickle.dump(data, f)
    return data


def _generate_and_cache_regional_dataset():
    """Generate realistic NHS regional dataset and cache it."""
    cache_file = CACHE_DIR / "regional_dataset.pkl"

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    # Generate realistic NHS data
    np.random.seed(42)
    n_sites = 200
    n_months = 12
    records_per_site_month = 5000

    dates = pd.date_range("2024-01-01", periods=n_months, freq="MS")
    regions = ["North East", "North West", "Yorkshire", "Midlands", "East", "London", "South"]

    rows = []
    for month in dates:
        for site_id in range(n_sites):
            region = regions[site_id % len(regions)]
            n_records = np.random.poisson(records_per_site_month)

            rows.extend(
                [
                    {
                        "month": month,
                        "region": region,
                        "site": f"Site_{site_id:03d}",
                        "referral_type": np.random.choice(["urgent", "routine"]),
                        "waiting_days": np.random.gamma(2, 15),
                    }
                    for _ in range(n_records)
                ]
            )

    data = pd.DataFrame(rows)

    # Cache to pickle
    with open(cache_file, "wb") as f:
        pickle.dump(data, f)
    return data


@pytest.fixture
def large_dataset():
    """Create a large dataset for performance testing (1M rows)."""
    return _generate_and_cache_large_dataset()


@pytest.fixture
def metric_methods():
    """Register test metric methods."""

    @metric_method
    def count_records(data):
        return len(data)

    @metric_method
    def sum_values(data):
        return data["value"].sum()

    @metric_method
    def mean_amount(data):
        return data["amount"].mean()

    @metric_method
    def category_distribution(data):
        return data.groupby("category").size()

    return {
        "count_records": count_records,
        "sum_values": sum_values,
        "mean_amount": mean_amount,
        "category_distribution": category_distribution,
    }


class TestLargeDatasetPerformance:
    """Test performance with large datasets."""

    def test_single_split_on_large_dataset(self, large_dataset, metric_methods):
        """Test single split on 1M row dataset completes in reasonable time."""
        config = {"split_by": "region", "total_count": {"method": ["count_records"], "filter": {}}}

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["total_count", "count_records"]

        assert isinstance(result, SeriesResult), f"Expected SeriesResult, got {type(result)}"
        assert len(result.data) == 10, f"Expected 10 regions, got {len(result.data)}"
        assert elapsed < 5.0, f"Processing took {elapsed:.2f}s, expected < 5.0s"

    def test_double_split_on_large_dataset(self, large_dataset, metric_methods):
        """Test double split creating hundreds of groups."""
        config = {
            "split_by": ["region", "site"],
            "total_count": {"method": ["count_records"], "filter": {}},
        }

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["total_count", "count_records"]

        assert isinstance(result, DataFrameResult), f"Expected DataFrameResult, got {type(result)}"
        # Should have up to 10*50=500 groups
        assert len(result.data) <= 500, f"Expected <= 500 groups, got {len(result.data)}"
        assert elapsed < 10.0, f"Processing took {elapsed:.2f}s, expected < 10.0s"

    def test_multiple_metrics_with_split(self, large_dataset, metric_methods):
        """Test multiple metrics with splitting on large dataset."""
        config = {
            "split_by": "region",
            "count": {"method": ["count_records"], "filter": {}},
            "total_value": {"method": ["sum_values"], "filter": {}},
            "avg_amount": {"method": ["mean_amount"], "filter": {}},
        }

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        # Check store has all metrics
        assert len(store) == 3, f"Expected 3 metrics, got {len(store)}"

        # Check all results are SeriesResult
        for metric, method in store._results:
            result = store[metric, method]
            assert isinstance(result, SeriesResult), (
                f"Expected SeriesResult for {metric}/{method}, got {type(result)}"
            )

        # Check performance
        assert elapsed < 10.0, f"Processing took {elapsed:.2f}s, expected < 10.0s"


class TestManyGroupsPerformance:
    """Test performance when splits create many groups."""

    def test_many_splits_creates_large_result(self, large_dataset, metric_methods):
        """Test splitting by columns with many unique values."""
        config = {
            "split_by": ["region", "site", "category"],
            "count": {"method": ["count_records"], "filter": {}},
        }

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["count", "count_records"]

        # Verify result type
        assert isinstance(result, DataFrameResult), f"Expected DataFrameResult, got {type(result)}"

        # Up to 10*50*5 = 2500 groups
        assert len(result.data) <= 2500, f"Expected <= 2500 groups, got {len(result.data)}"

        # Verify performance
        assert elapsed < 15.0, f"Processing took {elapsed:.2f}s, expected < 15.0s"

    def test_series_result_with_many_groups(self, large_dataset, metric_methods):
        """Test series result that creates large DataFrame when split."""
        config = {
            "split_by": ["region", "site"],
            "category_dist": {"method": ["category_distribution"], "filter": {}},
        }

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["category_dist", "category_distribution"]

        # Verify result type
        assert isinstance(result, DataFrameResult), f"Expected DataFrameResult, got {type(result)}"

        # Each of ~500 region/site combinations has 5 categories
        # So potentially up to 2500 rows
        assert len(result.data) <= 2500, f"Expected <= 2500 rows, got {len(result.data)}"

        # Verify performance
        assert elapsed < 15.0, f"Processing took {elapsed:.2f}s, expected < 15.0s"


class TestSplitWithFiltersPerformance:
    """Test performance when combining splits with filters."""

    def test_split_with_selective_filter(self, large_dataset, metric_methods):
        """Test split with filter that reduces data significantly."""
        config = {
            "split_by": "region",
            "category_a_count": {
                "method": ["count_records"],
                "filter": {"category": "A"},  # Should keep ~20% of data
            },
        }

        start_time = time.time()
        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["category_a_count", "count_records"]

        # Verify result type and structure
        assert isinstance(result, SeriesResult), f"Expected SeriesResult, got {type(result)}"
        assert len(result.data) == 10, f"Expected 10 regions, got {len(result.data)}"

        # Should be faster than processing all data
        assert elapsed < 3.0, f"Processing took {elapsed:.2f}s, expected < 3.0s"


class TestMemoryEfficiency:
    """Test memory efficiency of splitting operations."""

    def test_split_doesnt_duplicate_large_dataframe(self, large_dataset, metric_methods):
        """Ensure groupby doesn't create excessive memory copies."""
        # Get initial memory usage
        initial_size = sys.getsizeof(large_dataset)

        config = {"split_by": "region", "count": {"method": ["count_records"], "filter": {}}}

        store = interpret_metric_instructions(large_dataset, config, metric_methods)
        result = store["count", "count_records"]

        # Result should be much smaller than original data
        result_size = sys.getsizeof(result.data)
        assert result_size < initial_size / 100, (
            f"Result size ({result_size / 1024:.1f}KB) should be <1% of "
            f"original data ({initial_size / 1024 / 1024:.1f}MB)"
        )


class TestScalability:
    """Test scalability with varying data sizes."""

    @pytest.mark.parametrize(
        ("n_rows", "max_time"),
        [
            (10_000, 0.5),
            (100_000, 1.0),
            (1_000_000, 5.0),
        ],
    )
    def test_scaling_with_data_size(self, n_rows, max_time, metric_methods):
        """Test that processing time scales reasonably with data size."""
        np.random.seed(42)
        data = pd.DataFrame(
            {
                "region": np.random.choice(["R1", "R2", "R3", "R4", "R5"], n_rows),
                "value": np.random.randint(1, 100, n_rows),
            }
        )

        config = {"split_by": "region", "count": {"method": ["count_records"], "filter": {}}}

        start_time = time.time()
        store = interpret_metric_instructions(data, config, metric_methods)
        elapsed = time.time() - start_time

        result = store["count", "count_records"]

        # Verify result type
        assert isinstance(result, SeriesResult), f"Expected SeriesResult, got {type(result)}"

        # Verify performance
        assert elapsed < max_time, (
            f"Processing {n_rows:,} rows took {elapsed:.3f}s, expected < {max_time}s"
        )


class TestRealWorldScenario:
    """Test realistic NHS use case scenarios."""

    def test_regional_metrics_scenario(self, metric_methods):
        """
        Simulate realistic NHS regional metrics:
        - 7 NHS England regions
        - 200+ sites
        - 12 months of data
        - Multiple metrics
        """
        # Load cached dataset (generated and saved to parquet)
        data = _generate_and_cache_regional_dataset()

        config = {
            "split_by": "region",
            "total_referrals": {"method": ["count_records"], "filter": {}},
            "urgent_referrals": {
                "method": ["count_records"],
                "filter": {"referral_type": "urgent"},
            },
        }

        start_time = time.time()
        store = interpret_metric_instructions(data, config, metric_methods)
        elapsed = time.time() - start_time

        total = store["total_referrals", "count_records"]
        urgent = store["urgent_referrals", "count_records"]

        # Verify result types
        assert isinstance(total, SeriesResult), (
            f"Expected SeriesResult for total, got {type(total)}"
        )
        assert isinstance(urgent, SeriesResult), (
            f"Expected SeriesResult for urgent, got {type(urgent)}"
        )

        # Verify structure
        assert len(total.data) == 7, f"Expected 7 regions, got {len(total.data)}"

        # Verify performance
        assert elapsed < 10.0, f"Processing took {elapsed:.2f}s, expected < 10.0s"

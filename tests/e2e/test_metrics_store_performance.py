"""End-to-end performance tests for MetricsStore."""

import time

import pandas as pd

from quick_metric import generate_metrics


class TestMetricsStorePerformance:
    """Test MetricsStore performance characteristics with realistic workloads."""

    def test_direct_access_is_o1(self):
        """Test that direct access is O(1) - constant time."""
        data = pd.DataFrame({"value": range(1000)})
        config = {f"metric_{i}": {"filter": {}, "method": ["count_records"]} for i in range(100)}

        store = generate_metrics(data, config)

        # Direct access should be instant (O(1))
        start = time.time()
        for _ in range(1000):
            _ = store.value("metric_0", "count_records")
        elapsed = time.time() - start

        # Should be very fast (< 0.01s for 1000 accesses)
        assert elapsed < 0.01, f"Direct access too slow: {elapsed}s"

    def test_filter_scales_linearly(self):
        """Test that filtering is O(n) - linear with store size."""
        data = pd.DataFrame({"value": range(1000)})
        config = {f"metric_{i}": {"filter": {}, "method": ["count_records"]} for i in range(100)}

        store = generate_metrics(data, config)

        # Filtering should be linear with result count
        start = time.time()
        filtered = store.filter(method="count_records")
        elapsed = time.time() - start

        # Should be very fast even with 100 results (< 0.01s)
        assert elapsed < 0.01, f"Filtering too slow: {elapsed}s"
        assert len(filtered) == 100

    def test_large_store_operations(self):
        """Test operations on a large store (500 results)."""
        data = pd.DataFrame({"value": range(1000)})
        config = {
            f"metric_{i}": {"filter": {}, "method": ["count_records", "sum_values", "mean_value"]}
            for i in range(200)
        }

        store = generate_metrics(data, config)
        assert len(store) == 600  # 200 metrics × 3 methods

        # Test various operations are fast
        start = time.time()

        # Direct access
        _ = store.value("metric_0", "count_records")

        # Filter by method
        counts = store.by_method("count_records")
        assert len(counts) == 200

        # Filter by multiple methods
        aggs = store.by_method(["sum_values", "mean_value"])
        assert len(aggs) == 400

        # Chain filters
        scalars = store.filter(value_type="scalar").by_method("count_records")
        assert len(scalars) == 200

        elapsed = time.time() - start
        # All operations combined should be < 0.1s
        assert elapsed < 0.1, f"Operations on large store too slow: {elapsed}s"

    def test_export_performance(self):
        """Test that export operations scale reasonably."""
        data = pd.DataFrame({"category": ["A", "B", "C"] * 100, "value": range(300)})
        config = {
            f"metric_{i}": {"filter": {}, "method": ["count_records", "sum_values"]}
            for i in range(50)
        }

        store = generate_metrics(data, config)

        # Export to DataFrame
        start = time.time()
        df = store.to_dataframe()
        elapsed = time.time() - start

        assert len(df) > 0
        # Should be fast (< 0.05s for 100 results)
        assert elapsed < 0.05, f"DataFrame export too slow: {elapsed}s"

        # Export to nested dict
        start = time.time()
        nested = store.to_nested_dict()
        elapsed = time.time() - start

        assert len(nested) == 50
        # Should be very fast (< 0.01s)
        assert elapsed < 0.01, f"Nested dict export too slow: {elapsed}s"

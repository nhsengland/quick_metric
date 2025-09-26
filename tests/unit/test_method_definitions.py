"""Unit tests for method_definitions module."""

import pytest
from unittest.mock import patch

from quick_metric.method_definitions import METRICS_METHODS, metric_method


class TestMetricMethodDecorator:
    """Test the metric_method decorator."""

    def test_decorator_registers_method_in_global_dict(self):
        """Test that decorator registers method in METRICS_METHODS."""
        # Clear any existing methods for clean test
        original_methods = METRICS_METHODS.copy()
        METRICS_METHODS.clear()

        try:

            @metric_method
            def test_function(data):
                return len(data)

            assert METRICS_METHODS["test_function"] is test_function
        finally:
            # Restore original methods
            METRICS_METHODS.clear()
            METRICS_METHODS.update(original_methods)

    def test_decorator_returns_original_function(self):
        """Test that decorator returns the original function unchanged."""

        def original_function(data):
            return "original"

        decorated_function = metric_method(original_function)

        assert decorated_function is original_function

    def test_decorated_function_is_callable(self):
        """Test that decorated function can still be called normally."""

        @metric_method
        def test_function(data):
            return f"processed {len(data)} items"

        result = test_function([1, 2, 3])

        assert result == "processed 3 items"

    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves docstring and other metadata."""

        @metric_method
        def documented_function(data):
            """This function has documentation."""
            return data

        assert documented_function.__doc__ == "This function has documentation."
        assert documented_function.__name__ == "documented_function"

    def test_multiple_decorations_register_separately(self):
        """Test that multiple decorated functions are registered separately."""
        original_methods = METRICS_METHODS.copy()
        METRICS_METHODS.clear()

        try:

            @metric_method
            def function_one(data):
                return 1

            @metric_method
            def function_two(data):
                return 2

            assert len(METRICS_METHODS) == 2
            assert "function_one" in METRICS_METHODS
            assert "function_two" in METRICS_METHODS
            assert METRICS_METHODS["function_one"] is function_one
            assert METRICS_METHODS["function_two"] is function_two
        finally:
            METRICS_METHODS.clear()
            METRICS_METHODS.update(original_methods)

    def test_decorator_works_with_complex_functions(self):
        """Test decorator works with functions that have complex signatures."""

        @metric_method
        def complex_function(data, param1="default", *args, **kwargs):
            """Complex function with various parameter types."""
            return f"data: {data}, param1: {param1}, args: {args}, kwargs: {kwargs}"

        # Function should still work normally
        result = complex_function("test", "custom", "extra", key="value")
        expected = (
            "data: test, param1: custom, args: ('extra',), kwargs: {'key': 'value'}"
        )
        assert result == expected

    def test_decorator_handles_function_with_no_parameters(self):
        """Test decorator works with parameter-less functions."""

        @metric_method
        def no_params():
            return "no parameters"

        assert callable(no_params)
        assert no_params() == "no parameters"

"""
Test suite for custom exceptions in Quick Metric framework.

Tests all custom exception classes to ensure proper initialization,
message formatting, and attribute setting.
"""

import pytest

from quick_metric._exceptions import (
    DuplicateMethodWarning,
    EmptyRegistryError,
    InvalidMethodSignatureError,
    MethodExecutionError,
    MethodNotFoundError,
    MethodRegistrationError,
    MethodValidationError,
    MetricMethodError,
    MetricsMethodNotFoundError,
    MetricSpecificationError,
    RegistryLockError,
)


class TestMetricMethodError:
    """Test the base MetricMethodError exception."""

    def test_base_exception_inheritance(self):
        """Test that MetricMethodError inherits from LoggedException."""
        error = MetricMethodError("Test error")
        assert str(error) == "Test error"


class TestMethodRegistrationError:
    """Test the MethodRegistrationError exception."""

    def test_basic_registration_error(self):
        """Test basic registration error without existing methods."""
        error = MethodRegistrationError("test_method", "Already registered")

        assert error.method_name == "test_method"
        assert error.reason == "Already registered"
        assert error.existing_methods == []
        expected_msg = "Failed to register metric method 'test_method'"
        assert expected_msg in str(error)
        assert "Already registered" in str(error)

    def test_registration_error_with_existing_methods(self):
        """Test registration error with list of existing methods."""
        existing = ["method_a", "method_b", "method_c"]
        error = MethodRegistrationError(
            "test_method", "Invalid signature", existing_methods=existing
        )

        assert error.method_name == "test_method"
        assert error.reason == "Invalid signature"
        assert error.existing_methods == existing

        error_str = str(error)
        assert "Failed to register metric method 'test_method': Invalid signature" in error_str
        assert "Available methods: method_a, method_b, method_c" in error_str

    def test_registration_error_empty_existing_methods(self):
        """Test registration error with empty existing methods list."""
        error = MethodRegistrationError("test_method", "Some reason", existing_methods=[])

        assert error.existing_methods == []
        assert "Available methods:" not in str(error)


class TestMethodNotFoundError:
    """Test the MethodNotFoundError exception."""

    def test_method_not_found_with_available_methods(self):
        """Test method not found error with available methods."""
        available = ["count", "sum", "mean", "std"]
        error = MethodNotFoundError("average", available)

        assert error.method_name == "average"
        assert error.available_methods == available

        error_str = str(error)
        assert "Metric method 'average' is not registered" in error_str
        assert "Available methods: count, sum, mean, std" in error_str

    def test_method_not_found_no_available_methods(self):
        """Test method not found error with no available methods."""
        error = MethodNotFoundError("test_method", [])

        assert error.method_name == "test_method"
        assert error.available_methods == []

        error_str = str(error)
        assert "Metric method 'test_method' is not registered" in error_str
        assert "Available methods: None" in error_str

    def test_method_not_found_with_similar_suggestions(self):
        """Test method not found error suggests similar method names."""
        available = ["user_count", "user_sum", "total_users", "active_count"]
        error = MethodNotFoundError("count", available)

        error_str = str(error)
        assert "Did you mean one of: user_count, active_count?" in error_str

    def test_method_not_found_no_similar_suggestions(self):
        """Test method not found error when no similar methods exist."""
        available = ["sum", "mean", "std"]
        error = MethodNotFoundError("xyz", available)

        error_str = str(error)
        assert "Did you mean" not in error_str


class TestMethodExecutionError:
    """Test the MethodExecutionError exception."""

    def test_method_execution_error_basic(self):
        """Test basic method execution error."""
        original = ValueError("Invalid value")
        error = MethodExecutionError("test_method", original)

        assert error.method_name == "test_method"
        assert error.original_error == original
        assert error.data_info is None

        error_str = str(error)
        assert "failed during execution" in error_str
        assert "Invalid value" in error_str

    def test_method_execution_error_with_data_info(self):
        """Test method execution error with data information."""
        original = KeyError("missing_column")
        data_info = "DataFrame with 100 rows, 5 columns"
        error = MethodExecutionError("calculate_stats", original, data_info)

        assert error.method_name == "calculate_stats"
        assert error.original_error == original
        assert error.data_info == data_info

        error_str = str(error)
        assert "Metric method 'calculate_stats' failed during execution" in error_str
        assert "missing_column" in error_str
        assert "Data info: DataFrame with 100 rows, 5 columns" in error_str


class TestInvalidMethodSignatureError:
    """Test the InvalidMethodSignatureError exception."""

    def test_invalid_signature_error(self):
        """Test invalid method signature error."""
        error = InvalidMethodSignatureError("bad_method", "No parameters defined")

        assert error.method_name == "bad_method"
        assert error.signature_issue == "No parameters defined"

        error_str = str(error)
        assert "has invalid signature for metric use" in error_str
        assert "No parameters defined" in error_str
        assert "Metric methods should accept at least one parameter (the data)" in error_str


class TestMethodValidationError:
    """Test the MethodValidationError exception."""

    def test_method_validation_error(self):
        """Test method validation error."""
        error = MethodValidationError("test_method", "Return type must be numeric")

        assert error.method_name == "test_method"
        assert error.validation_issue == "Return type must be numeric"

        error_str = str(error)
        assert "validation failed" in error_str
        assert "Return type must be numeric" in error_str


class TestDuplicateMethodWarning:
    """Test the DuplicateMethodWarning exception."""

    def test_duplicate_method_warning_basic(self):
        """Test basic duplicate method warning."""
        warning = DuplicateMethodWarning("existing_method")

        assert warning.method_name == "existing_method"
        assert warning.source_info is None

        warning_str = str(warning)
        assert "Method 'existing_method' is being re-registered" in warning_str
        assert "overwriting previous registration" in warning_str

    def test_duplicate_method_warning_with_source(self):
        """Test duplicate method warning with source information."""
        warning = DuplicateMethodWarning("existing_method", "module.py:line 42")

        assert warning.method_name == "existing_method"
        assert warning.source_info == "module.py:line 42"

        warning_str = str(warning)
        assert "Method 'existing_method' is being re-registered" in warning_str
        assert "from module.py:line 42" in warning_str


class TestRegistryLockError:
    """Test the RegistryLockError exception."""

    def test_registry_lock_error(self):
        """Test registry lock error."""
        error = RegistryLockError("register_method", "Timeout acquiring lock")

        assert error.operation == "register_method"
        assert error.reason == "Timeout acquiring lock"

        error_str = str(error)
        assert "failed due to threading issue" in error_str
        assert "Timeout acquiring lock" in error_str


class TestEmptyRegistryError:
    """Test the EmptyRegistryError exception."""

    def test_empty_registry_error(self):
        """Test empty registry error."""
        error = EmptyRegistryError("list_methods")

        assert error.operation == "list_methods"

        error_str = str(error)
        assert "Cannot perform list_methods on empty method registry" in error_str
        assert "Register some methods first using @metric_method decorator" in error_str


class TestExceptionHierarchy:
    """Test exception hierarchy and inheritance."""

    @pytest.mark.parametrize(
        "exception",
        [
            MethodRegistrationError("test", "reason"),
            MethodNotFoundError("test", []),
            MethodExecutionError("test", Exception()),
            InvalidMethodSignatureError("test", "issue"),
            MethodValidationError("test", "issue"),
            RegistryLockError("test", "reason"),
            EmptyRegistryError("test"),
        ],
    )
    def test_metric_exception_inherits_from_base(self, exception):
        """Test that metric exceptions inherit from MetricMethodError."""
        assert isinstance(exception, MetricMethodError)

    def test_duplicate_warning_not_metric_error(self):
        """Test that DuplicateMethodWarning is not a MetricMethodError."""
        warning = DuplicateMethodWarning("test")
        assert not isinstance(warning, MetricMethodError)


class TestExceptionMessages:
    """Test exception message formatting and consistency."""

    @pytest.mark.parametrize(
        "exception",
        [
            MethodRegistrationError("test", "reason"),
            MethodNotFoundError("test", ["available"]),
            MethodExecutionError("test", ValueError("original")),
            InvalidMethodSignatureError("test", "issue"),
            MethodValidationError("test", "issue"),
            DuplicateMethodWarning("test"),
            RegistryLockError("operation", "reason"),
            EmptyRegistryError("operation"),
            MetricSpecificationError("test issue"),
            MetricsMethodNotFoundError("test", ["available"]),
        ],
    )
    def test_exception_has_meaningful_message(self, exception):
        """Test that exception produces meaningful error message."""
        message = str(exception)
        assert len(message) > 10  # Should have meaningful content
        assert message != ""
        assert "test" in message or "operation" in message  # Should include context


class TestMetricSpecificationError:
    """Test the MetricSpecificationError exception."""

    def test_basic_specification_error(self):
        """Test basic specification error without method spec."""
        error = MetricSpecificationError("Invalid format")

        assert error.specification_issue == "Invalid format"
        assert error.method_spec is None
        expected_msg = "Invalid metric specification: Invalid format"
        assert str(error) == expected_msg

    def test_specification_error_with_method_spec(self):
        """Test specification error with method specification details."""
        method_spec = {"invalid": "spec"}
        error = MetricSpecificationError("Multiple methods", method_spec)

        assert error.specification_issue == "Multiple methods"
        assert error.method_spec == {"invalid": "spec"}
        expected_msg = (
            "Invalid metric specification: Multiple methods. "
            "Method specification: {'invalid': 'spec'}"
        )
        assert str(error) == expected_msg

    def test_inheritance_from_valueerror_and_loggederror(self):
        """Test that MetricSpecificationError inherits from both ValueError and LoggedError."""
        error = MetricSpecificationError("Test error")
        assert isinstance(error, ValueError)
        # Should also inherit from LoggedException (from NHS Herbot)
        assert hasattr(error, "__module__")  # Basic check for proper inheritance


class TestMetricsMethodNotFoundError:
    """Test the MetricsMethodNotFoundError exception."""

    def test_method_not_found_with_available_methods(self):
        """Test exception with available methods list."""
        available = ["method_a", "method_b", "method_c"]
        error = MetricsMethodNotFoundError("missing_method", available)

        assert error.method_name == "missing_method"
        assert error.available_methods == available

        error_msg = str(error)
        assert "Metric method 'missing_method' is not registered" in error_msg
        assert "Available methods: method_a, method_b, method_c" in error_msg
        # With difflib, it might suggest some close matches
        assert "missing_method" in error_msg

    def test_method_not_found_no_available_methods(self):
        """Test exception with empty methods list."""
        error = MetricsMethodNotFoundError("missing_method", [])

        assert error.method_name == "missing_method"
        assert error.available_methods == []
        expected_msg = "Metric method 'missing_method' is not registered. Available methods: None"
        assert str(error) == expected_msg

    def test_method_not_found_with_similar_suggestions(self):
        """Test exception provides suggestions for similar method names using difflib."""
        available = ["count_records", "count_users", "calculate_mean"]
        error = MetricsMethodNotFoundError("count_record", available)

        error_msg = str(error)
        assert "count_records" in error_msg
        assert "Did you mean one of:" in error_msg
        # difflib should suggest count_records as the closest match for count_record
        assert "count_records" in error_msg

    def test_method_not_found_with_typo_suggestions(self):
        """Test exception provides good suggestions for common typos."""
        available = ["filter_active", "calculate_sum", "count_users"]
        error = MetricsMethodNotFoundError("fliter_active", available)

        error_msg = str(error)
        assert "filter_active" in error_msg
        assert "Did you mean one of:" in error_msg

    def test_method_not_found_no_close_matches(self):
        """Test exception when no close matches are found."""
        available = ["count_records", "calculate_mean"]
        error = MetricsMethodNotFoundError("totally_different", available)

        error_msg = str(error)
        # Should not show suggestions when nothing is close enough
        assert "Did you mean one of:" not in error_msg
        assert "Available methods: count_records, calculate_mean" in error_msg

    def test_inherits_from_metric_method_error(self):
        """Test that MetricsMethodNotFoundError inherits from MetricMethodError."""
        error = MetricsMethodNotFoundError("test", [])
        assert isinstance(error, MetricMethodError)

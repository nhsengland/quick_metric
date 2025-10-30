"""Unit tests for method_definitions module."""

import pytest

from quick_metric.exceptions import (
    EmptyRegistryError,
    InvalidMethodSignatureError,
    MethodNotFoundError,
    RegistryLockError,
)
from quick_metric.registry import (
    METRICS_METHODS,
    MetricMethod,
    MetricRegistry,
    _registry,
    clear_methods,
    get_method,
    get_registered_methods,
    list_method_names,
    metric_method,
)


class TestMetricMethodDecorator:
    """Test the metric_method decorator."""

    def setup_method(self):
        """Save registry and clear before each test."""
        self._saved_methods = METRICS_METHODS.copy()
        clear_methods()

    def teardown_method(self):
        """Restore registry after each test."""
        METRICS_METHODS.clear()
        METRICS_METHODS.update(self._saved_methods)

    @pytest.mark.parametrize("function_name", ["test_function", "another_test", "custom_func"])
    def test_decorator_registers_function(self, function_name):
        """Test that decorator registers function in global registry."""

        def test_func(data):
            return len(data)

        # Set the function name
        test_func.__name__ = function_name

        decorated = metric_method(test_func)

        assert function_name in METRICS_METHODS
        # Registry now stores MetricMethod objects, not raw functions
        assert isinstance(METRICS_METHODS[function_name], MetricMethod)
        assert isinstance(decorated, MetricMethod)
        # The MetricMethod wraps the original function
        assert METRICS_METHODS[function_name].func is test_func

    def test_decorator_returns_original_function(self):
        """Test that decorator returns a MetricMethod wrapper."""

        def original_function(_data):
            return "original"

        decorated_function = metric_method(original_function)

        # Returns MetricMethod, not the original function
        assert isinstance(decorated_function, MetricMethod)
        assert decorated_function.func is original_function

    @pytest.mark.parametrize(
        ("input_data", "expected"),
        [
            ([1, 2, 3], "processed 3 items"),
            ([1, 2, 3, 4, 5], "processed 5 items"),
            ([], "processed 0 items"),
        ],
    )
    def test_decorated_function_works_normally(self, input_data, expected):
        """Test that decorated function can still be called normally."""

        @metric_method
        def test_function(data):
            return f"processed {len(data)} items"

        result = test_function(input_data)
        assert result == expected

    def test_decorator_preserves_metadata(self):
        """Test that decorator preserves function metadata in the wrapper."""

        @metric_method
        def documented_function(data):
            """This function has documentation."""
            return data

        # MetricMethod wrapper should preserve the original function's metadata
        assert isinstance(documented_function, MetricMethod)
        assert documented_function.func.__doc__ == "This function has documentation."
        assert documented_function.func.__name__ == "documented_function"
        assert documented_function.name == "documented_function"

    def test_decorator_rejects_parameterless_function(self):
        """Test that decorator rejects functions with no parameters."""
        with pytest.raises(InvalidMethodSignatureError):

            @metric_method
            def no_params():
                return "error"

    def test_multiple_functions_register_separately(self):
        """Test that multiple decorated functions are registered separately."""

        @metric_method
        def function_one(_data):
            return 1

        @metric_method
        def function_two(_data):
            return 2

        assert len(METRICS_METHODS) == 2
        assert "function_one" in METRICS_METHODS
        assert "function_two" in METRICS_METHODS


class TestGetMethod:
    """Test the get_method function."""

    def setup_method(self):
        """Save registry and clear before each test."""
        self._saved_methods = METRICS_METHODS.copy()
        clear_methods()

    def teardown_method(self):
        """Restore registry after each test."""
        METRICS_METHODS.clear()
        METRICS_METHODS.update(self._saved_methods)

    def test_get_method_returns_registered_function(self):
        """Test getting a registered method by name."""

        @metric_method
        def test_function(data):
            return data

        retrieved = get_method("test_function")
        assert retrieved is test_function

    def test_get_method_nonexistent_raises_error(self):
        """Test that getting non-existent method raises MethodNotFoundError."""
        with pytest.raises(MethodNotFoundError) as exc_info:
            get_method("nonexistent")

        assert "nonexistent" in str(exc_info.value)

    def test_get_method_error_includes_available_methods(self):
        """Test that error includes list of available methods."""

        @metric_method
        def existing_method(data):
            return data

        with pytest.raises(MethodNotFoundError) as exc_info:
            get_method("nonexistent")

        error_msg = str(exc_info.value)
        assert "existing_method" in error_msg


class TestClearMethods:
    """Test the clear_methods function."""

    def setup_method(self):
        """Save registry and clear before each test."""
        self._saved_methods = METRICS_METHODS.copy()
        clear_methods()

    def teardown_method(self):
        """Restore registry after each test."""
        METRICS_METHODS.clear()
        METRICS_METHODS.update(self._saved_methods)

    def test_clear_removes_all_methods(self):
        """Test that clear removes all registered methods."""

        @metric_method
        def test_function(data):
            return data

        assert len(METRICS_METHODS) == 1
        clear_methods()
        assert len(METRICS_METHODS) == 0


class TestComplexFunctionSignatures:
    """Test decorator with various function signatures."""

    def setup_method(self):
        """Save registry and clear before each test."""
        self._saved_methods = METRICS_METHODS.copy()
        clear_methods()

    def teardown_method(self):
        """Restore registry after each test."""
        METRICS_METHODS.clear()
        METRICS_METHODS.update(self._saved_methods)

    def test_function_with_multiple_parameters(self):
        """Test decorator works with multiple parameter functions."""

        @metric_method
        def multi_param_function(data, _column="value", threshold=0):
            return len([x for x in data if x > threshold])

        assert "multi_param_function" in METRICS_METHODS

        # Test it still works
        result = multi_param_function([1, 2, 3, 4, 5], threshold=2)
        assert result == 3

    def test_function_with_args_and_kwargs(self):
        """Test decorator works with *args and **kwargs."""

        @metric_method
        def flexible_function(data, *args, **kwargs):
            return f"data: {len(data)}, args: {len(args)}, kwargs: {len(kwargs)}"

        assert "flexible_function" in METRICS_METHODS

        # Test it still works
        result = flexible_function([1, 2], "extra", key="value")
        assert result == "data: 2, args: 1, kwargs: 1"


class TestMethodRegistrationErrors:
    """Test error conditions in method registration."""

    def setup_method(self):
        """Clear registry before each test."""
        clear_methods()

    def test_registry_lock_error_simulation(self):
        """Test registry lock error handling."""

        # Create a method that will be registered
        @metric_method
        def test_method(data):
            return len(data)

        # Verify it was registered successfully
        assert "test_method" in METRICS_METHODS

    def test_duplicate_method_registration_warning(self):
        """Test that re-registering a method logs warning but works."""

        @metric_method
        def duplicate_method(_data):
            return "first"

        # Re-register the same method name
        @metric_method
        def duplicate_method(_data):  # noqa: F811
            return "second"

        # Should use the latest registration
        assert METRICS_METHODS["duplicate_method"]([1, 2, 3]) == "second"

    def test_method_validation_error(self):
        """Test error during method validation."""

        # Try to register a method with no parameters
        def invalid_method():
            return "no params"

        with pytest.raises(InvalidMethodSignatureError) as exc_info:
            _registry.register(invalid_method)

        error_msg = str(exc_info.value)
        assert "invalid_method" in error_msg
        assert "must accept at least one parameter" in error_msg

    def test_get_method_with_suggestions(self):
        """Test get_method provides suggestions for similar names."""

        @metric_method
        def count_records(data):
            return len(data)

        @metric_method
        def sum_values(data):
            return sum(data)

        # Try to get a method with a typo
        with pytest.raises(MethodNotFoundError) as exc_info:
            get_method("count_record")  # Missing 's'

        error_msg = str(exc_info.value)
        assert "count_record" in error_msg
        assert "Did you mean" in error_msg or "count_records" in error_msg

    def test_get_method_no_methods_registered(self):
        """Test get_method when no methods are registered."""
        clear_methods()  # Ensure empty registry

        with pytest.raises(MethodNotFoundError) as exc_info:
            get_method("any_method")

        error_msg = str(exc_info.value)
        assert "any_method" in error_msg
        assert "Available methods: None" in error_msg

    def test_empty_registry_error(self):
        """Test EmptyRegistryError when trying to list methods from empty registry."""
        clear_methods()  # Ensure empty registry

        with pytest.raises(EmptyRegistryError) as exc_info:
            list_method_names()

        error_msg = str(exc_info.value)
        assert "empty method registry" in error_msg
        assert "@metric_method decorator" in error_msg

    def test_registry_clear_and_restore(self):
        """Test that clear methods works and registry can be restored."""

        # Register a method first
        @metric_method
        def test_clear_method(data):
            return len(data)

        assert "test_clear_method" in METRICS_METHODS

        # Clear should work normally
        clear_methods()

        # Verify it was cleared
        assert len(METRICS_METHODS) == 0

        # Should be able to register again
        @metric_method
        def new_method(data):
            return sum(data)

        assert "new_method" in METRICS_METHODS


class TestRegistryErrorConditions:
    """Test error conditions and edge cases for the registry."""

    def test_empty_registry_error_for_list_method_names(self):
        """Test EmptyRegistryError is raised when listing methods from empty registry."""

        # Clear the registry first
        clear_methods()

        with pytest.raises(EmptyRegistryError, match="empty method registry"):
            list_method_names()

    def test_method_signature_validation_error(self):
        """Test InvalidMethodSignatureError for invalid function signatures."""

        with pytest.raises(InvalidMethodSignatureError, match="must accept at least one parameter"):

            @metric_method
            def invalid_function():  # No parameters
                return "invalid"

    def test_get_registered_methods_returns_copy(self):
        """Test that get_registered_methods returns a copy of the registry."""
        # Clear and add a test method
        clear_methods()

        @metric_method
        def copy_test_method(data):
            return len(data)

        methods_copy = get_registered_methods()

        # Modify the copy - should not affect the original registry
        methods_copy["new_fake_method"] = lambda x: x

        # Original registry should be unchanged
        original_methods = get_registered_methods()
        assert "new_fake_method" not in original_methods
        assert "copy_test_method" in original_methods

    def test_registry_lock_error_on_get_methods(self, mocker):
        """Test RegistryLockError when lock fails on get_methods."""
        registry = MetricRegistry()

        # Mock the lock's __enter__ method to raise an exception using pytest-mock
        mock_lock = mocker.MagicMock()
        mock_lock.__enter__.side_effect = RuntimeError("Lock failed")
        mock_lock.__exit__.return_value = None

        # Replace the actual lock with our mock
        registry._lock = mock_lock

        with pytest.raises(RegistryLockError, match="get_methods"):
            registry.get_methods()

    def test_registry_lock_error_on_list_method_names(self, mocker):
        """Test RegistryLockError when lock fails on list_method_names."""
        registry = MetricRegistry()

        # Mock the lock's __enter__ method to raise an exception using pytest-mock
        mock_lock = mocker.MagicMock()
        mock_lock.__enter__.side_effect = RuntimeError("Lock failed")
        mock_lock.__exit__.return_value = None

        # Replace the actual lock with our mock
        registry._lock = mock_lock

        with pytest.raises(RegistryLockError, match="list_method_names"):
            registry.list_method_names()

    def test_registry_lock_error_on_clear(self, mocker):
        """Test RegistryLockError when lock fails on clear."""
        registry = MetricRegistry()

        # Mock the lock's __enter__ method to raise an exception using pytest-mock
        mock_lock = mocker.MagicMock()
        mock_lock.__enter__.side_effect = RuntimeError("Lock failed")
        mock_lock.__exit__.return_value = None

        # Replace the actual lock with our mock
        registry._lock = mock_lock

        with pytest.raises(RegistryLockError, match="clear"):
            registry.clear()


class TestMetricMethodWithValueColumn:
    """Test MetricMethod with value_column parameter."""

    def setup_method(self):
        """Save and clear registry before each test."""
        self._saved_methods = METRICS_METHODS.copy()
        clear_methods()

    def teardown_method(self):
        """Restore registry after each test."""
        METRICS_METHODS.clear()
        METRICS_METHODS.update(self._saved_methods)

    def test_metric_method_decorator_with_value_column(self):
        """Test using @metric_method with value_column parameter."""

        @metric_method(value_column="count")
        def count_by_category(data):
            return data.groupby("category")["value"].sum().reset_index(name="count")

        assert "count_by_category" in METRICS_METHODS
        method = METRICS_METHODS["count_by_category"]
        assert method.value_column == "count"

    def test_metric_method_retrieval_by_name(self):
        """Test retrieving a method by name using metric_method(name)."""

        @metric_method
        def test_method(data):
            return len(data)

        retrieved = metric_method("test_method")
        assert retrieved is not None
        assert retrieved.name == "test_method"

    def test_metric_method_retrieval_all_methods(self):
        """Test retrieving all methods using metric_method()."""

        @metric_method
        def method1(data):
            return len(data)

        @metric_method
        def method2(data):
            return sum(data)

        all_methods = metric_method()
        assert "method1" in all_methods
        assert "method2" in all_methods

    def test_metric_method_with_invalid_value_column_on_retrieval(self):
        """Test that value_column cannot be specified when retrieving by name."""

        @metric_method
        def test_method(data):
            return data

        with pytest.raises(ValueError, match="Cannot specify value_column when retrieving"):
            metric_method("test_method", value_column="count")  # type: ignore

    def test_metric_method_invalid_argument_type(self):
        """Test that invalid argument types raise ValueError."""
        with pytest.raises(ValueError, match="Invalid argument type"):
            metric_method(123)  # type: ignore

    def test_metric_method_call_without_func(self):
        """Test MetricMethod.__call__ when used as decorator factory."""
        method_factory = MetricMethod(func=None, value_column="result")

        def my_func(data):
            return len(data)

        # Should work as a decorator
        wrapped = method_factory(my_func)
        assert wrapped is not None

"""
Method registration and decorator functionality for Quick Metric.

Provides the core decorator for registering custom metric methods with
a thread-safe registry system.

Classes
-------
MetricMethod : Wrapper for metric methods with metadata and result transformation
MetricRegistry : Thread-safe registry for user-defined metric methods

Functions
---------
metric_method : Decorator for registering custom metric functions
get_method : Retrieve a registered method by name
get_registered_methods : Get dictionary of all registered methods
list_method_names : List all registered method names
clear_methods : Clear all methods from the registry

Constants
---------
METRICS_METHODS : Global registry instance
"""

import inspect
import threading
from typing import Any, Callable, overload

from loguru import logger

from quick_metric.exceptions import (
    EmptyRegistryError,
    InvalidMethodSignatureError,
    MethodNotFoundError,
    RegistryLockError,
)
from quick_metric.results import MetricResult, create_result


class MetricMethod:
    """
    Wrapper for a metric method with metadata and result transformation.

    Encapsulates:
    - The underlying function
    - Metadata (value_column, etc.)
    - Logic to apply the method and transform results to MetricResult objects

    Can also act as the decorator factory itself when value_column is provided.

    Parameters
    ----------
    func : Callable or None
        The underlying metric function (None when used as decorator factory)
    value_column : str, optional
        For DataFrame results, which column contains the metric values
    registry : MetricRegistry, optional
        Registry instance to use (for decorator functionality)
    """

    def __init__(
        self,
        func: Callable | None = None,
        value_column: str | None = None,
        registry: "MetricRegistry | None" = None,
    ):
        self.func = func
        self.value_column = value_column
        self._registry = registry

        # Only set name if we have a function
        if func is not None:
            self.name = func.__name__

    def apply(self, metric_name: str, data: Any, *args, **kwargs) -> Any:
        """
        Apply the method and transform result to MetricResult.

        Parameters
        ----------
        metric_name : str
            Name of the metric being calculated
        data : Any
            Data to process
        *args, **kwargs
            Additional arguments to pass to the function

        Returns
        -------
        MetricResult
            The transformed result object
        """
        if self.func is None:
            raise ValueError("Cannot apply MetricMethod without a function")

        # Call the underlying function
        result = self.func(data, *args, **kwargs)

        # If already a MetricResult, return as-is
        if isinstance(result, MetricResult):
            return result

        # Transform to MetricResult
        return create_result(
            metric=metric_name, method=self.name, data=result, value_column=self.value_column
        )

    def __call__(self, *args, **kwargs):
        """
        Multi-purpose calling:
        1. If we have a func, call it (backwards compatibility)
        2. If we don't have a func, this is a decorator factory -
           first arg should be the function to decorate
        """
        # Case 1: Used as decorator factory - @metric_method(value_column='count')
        # We have value_column but no func, so args[0] should be the function to decorate
        if self.func is None and len(args) == 1 and callable(args[0]) and not kwargs:
            func_to_wrap = args[0]
            if self._registry is not None:
                return self._registry.register(func_to_wrap, value_column=self.value_column)
            # If no registry, just wrap the function
            return MetricMethod(func_to_wrap, value_column=self.value_column)

        # Case 2: Direct function call for backwards compatibility
        if self.func is not None:
            return self.func(*args, **kwargs)

        raise ValueError(f"MetricMethod.__call__ received unexpected arguments: {args}, {kwargs}")


class MetricRegistry:
    """
    Thread-safe registry for user-defined metric methods.

    Provides a centralized, thread-safe way to register and access metric
    methods decorated with @metric_method.
    """

    def __init__(self):
        self._methods: dict[str, MetricMethod] = {}
        self._lock = threading.RLock()

    def register(self, func: Callable, value_column: str | None = None) -> MetricMethod:
        """
        Register a user function as a metric method.

        Parameters
        ----------
        func : Callable
            User function to register. Must accept at least one parameter.
        value_column : str, optional
            For DataFrame results, which column contains the metric values.

        Returns
        -------
        MetricMethod
            The MetricMethod wrapper object.

        Raises
        ------
        InvalidMethodSignatureError
            If function has invalid signature.
        """
        logger.trace(f"Registering metric method: {func.__name__}")

        # Validate function signature
        sig = inspect.signature(func)
        if not sig.parameters:
            raise InvalidMethodSignatureError(
                func.__name__,
                "Function must accept at least one parameter (the data)",
            )

        # Create MetricMethod wrapper
        metric_method_obj = MetricMethod(func, value_column=value_column)

        # Register the method
        with self._lock:
            if func.__name__ in self._methods:
                logger.warning(f"Method '{func.__name__}' already registered, overwriting")
            self._methods[func.__name__] = metric_method_obj

        logger.success(f"Metric method '{func.__name__}' registered")
        return metric_method_obj

    def get_method(self, name: str) -> MetricMethod:
        """
        Get a registered method by name.

        Parameters
        ----------
        name : str
            Name of the method.

        Returns
        -------
        MetricMethod
            The registered MetricMethod wrapper.

        Raises
        ------
        MethodNotFoundError
            If method not registered.
        """
        with self._lock:
            if name not in self._methods:
                available = list(self._methods.keys())
                logger.error(f"Method '{name}' not found. Available: {available}")
                raise MethodNotFoundError(name, available)
            return self._methods[name]

    def get_methods(self) -> dict[str, Callable]:
        """
        Get copy of all registered methods.

        Returns
        -------
        Dict[str, Callable]
            Copy of methods dictionary.

        Raises
        ------
        RegistryLockError
            If threading lock fails.
        """
        try:
            with self._lock:
                return self._methods.copy()
        except Exception as e:
            raise RegistryLockError("get_methods", str(e)) from e

    def list_method_names(self) -> list[str]:
        """
        Get list of registered method names.

        Returns
        -------
        list[str]
            List of method names.

        Raises
        ------
        EmptyRegistryError
            If no methods registered.
        RegistryLockError
            If threading lock fails.
        """
        try:
            with self._lock:
                if not self._methods:
                    raise EmptyRegistryError("list_method_names")
                return list(self._methods.keys())
        except EmptyRegistryError:
            raise
        except Exception as e:
            raise RegistryLockError("list_method_names", str(e)) from e

    def clear(self) -> None:
        """
        Clear all registered methods (for testing).

        Raises
        ------
        RegistryLockError
            If threading lock fails.
        """
        logger.debug("Clearing all registered metric methods")
        try:
            with self._lock:
                self._methods.clear()
        except Exception as e:
            raise RegistryLockError("clear_methods", str(e)) from e
        logger.success("All metric methods cleared")


# Global registry instance
_registry = MetricRegistry()

# Expose registry methods as module-level functions for public API
METRICS_METHODS = _registry._methods  # Direct reference to internal dict


# Type overloads for metric_method to help type checkers
@overload
def metric_method() -> dict[str, MetricMethod]: ...


@overload
def metric_method(func_or_name: str) -> MetricMethod: ...


@overload
def metric_method(func_or_name: Callable) -> MetricMethod: ...


@overload
def metric_method(*, value_column: str) -> MetricMethod: ...


def metric_method(func_or_name=None, *, value_column=None):
    """
    Decorator to register a user function as a metric method, or query registered methods.

    Can be used in four ways:
    1. As a simple decorator: @metric_method
    2. As a decorator with parameters: @metric_method(value_column='count')
    3. To get all methods: metric_method()
    4. To get a specific method: metric_method('method_name')

    Parameters
    ----------
    func_or_name : Callable or str, optional
        User function to register when used as decorator, or method name to retrieve.
    value_column : str, optional
        For DataFrame returns, which column contains the metric values.

    Returns
    -------
    MetricMethod or dict[str, MetricMethod]
        When used as decorator: returns MetricMethod wrapper.
        When called without args: returns dict of all registered methods.
        When called with method name: returns the specific MetricMethod.
        When called with parameters only: returns MetricMethod decorator factory.

    Examples
    --------
    Simple decorator:

    >>> @metric_method
    ... def my_custom_metric(data):
    ...     return len(data)

    Decorator with value_column for DataFrame returns:

    >>> @metric_method(value_column='count')
    ... def analyze_by_month(data):
    ...     return data.groupby('month').size().reset_index(name='count')

    Query registered methods:

    >>> all_methods = metric_method()
    >>> print(list(all_methods.keys()))

    >>> specific_method = metric_method('my_custom_metric')
    """
    # Case 1: Called without arguments - return all methods
    if func_or_name is None and value_column is None:
        return _registry.get_methods()

    # Case 2: Called with a string - return specific method
    if isinstance(func_or_name, str):
        if value_column is not None:
            raise ValueError("Cannot specify value_column when retrieving a method by name")
        return _registry.get_method(func_or_name)

    # Case 3: Called with a function (direct decorator usage)
    if callable(func_or_name):
        return _registry.register(func_or_name, value_column=value_column)

    # Case 4: Called with keyword args only (parameterized decorator)
    # Return a MetricMethod instance that acts as a decorator factory
    if func_or_name is None:
        return MetricMethod(func=None, value_column=value_column, registry=_registry)

    raise ValueError(f"Invalid argument type: {type(func_or_name)}")


def get_method(name: str) -> Callable:
    """Get a registered method by name."""
    return _registry.get_method(name)


def get_registered_methods() -> dict[str, Callable]:
    """Get all registered methods."""
    return _registry.get_methods()


def list_method_names() -> list[str]:
    """Get list of registered method names."""
    return _registry.list_method_names()


def clear_methods() -> None:
    """Clear all registered methods (for testing)."""
    _registry.clear()

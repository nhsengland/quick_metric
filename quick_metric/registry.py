"""
Method registration and decorator functionality for Quick Metric.

Provides the core decorator for registering custom metric methods with
a thread-safe registry system.

Classes
-------
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
from typing import Callable

from loguru import logger

from quick_metric.exceptions import (
    EmptyRegistryError,
    InvalidMethodSignatureError,
    MethodNotFoundError,
    RegistryLockError,
)


class MetricRegistry:
    """
    Thread-safe registry for user-defined metric methods.

    Provides a centralized, thread-safe way to register and access metric
    methods decorated with @metric_method.
    """

    def __init__(self):
        self._methods: dict[str, Callable] = {}
        self._lock = threading.RLock()

    def register(self, func: Callable) -> Callable:
        """
        Register a user function as a metric method.

        Parameters
        ----------
        func : Callable
            User function to register. Must accept at least one parameter.

        Returns
        -------
        Callable
            The original function, unchanged.

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

        # Register the method
        with self._lock:
            if func.__name__ in self._methods:
                logger.warning(f"Method '{func.__name__}' already registered, overwriting")
            self._methods[func.__name__] = func

        logger.success(f"Metric method '{func.__name__}' registered")
        return func

    def get_method(self, name: str) -> Callable:
        """
        Get a registered method by name.

        Parameters
        ----------
        name : str
            Name of the method.

        Returns
        -------
        Callable
            The registered method.

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


def metric_method(func_or_name=None):
    """
    Decorator to register a user function as a metric method, or query registered methods.

    Can be used in three ways:
    1. As a decorator: @metric_method
    2. To get all methods: metric_method()
    3. To get a specific method: metric_method('method_name')

    Parameters
    ----------
    func_or_name : Callable or str, optional
        User function to register when used as decorator, or method name to retrieve.

    Returns
    -------
    Callable or dict
        When used as decorator: returns the original function unchanged.
        When called without args: returns dict of all registered methods.
        When called with method name: returns the specific method.

    Examples
    --------
    As a decorator:

    ```python
    @metric_method
    def my_custom_metric(data):
        return len(data)
    ```

    To get all methods:

    ```python
    all_methods = metric_method()
    print(list(all_methods.keys()))
    ```

    To get a specific method:

    ```python
    my_method = metric_method('my_custom_metric')
    ```
    """
    # Case 1: Called without arguments - return all methods
    if func_or_name is None:
        return _registry.get_methods()

    # Case 2: Called with a string - return specific method
    if isinstance(func_or_name, str):
        return _registry.get_method(func_or_name)

    # Case 3: Called with a function (decorator usage)
    if callable(func_or_name):
        return _registry.register(func_or_name)

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

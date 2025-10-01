"""
Method registration and decorator functionality for Quick Metric.

This module provides the core decorator for registering custom metric methods
that can be used with the quick_metric framework. Methods decorated with
@metric_method are automatically registered in the global registry and become
available for use in YAML configurations.
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


def metric_method(func: Callable) -> Callable:
    """
    Decorator to register a user function as a metric method.

    Parameters
    ----------
    func : Callable
        User function to register.

    Returns
    -------
    Callable
        The original function, unchanged.

    Examples
    --------
    >>> @metric_method
    ... def my_custom_metric(data):
    ...     return len(data)
    """
    return _registry.register(func)


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

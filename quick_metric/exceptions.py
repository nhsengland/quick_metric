"""
Custom exceptions for Quick Metric framework.

Domain-specific exceptions with clear error messages and automatic logging.

Classes
-------
MetricMethodError : Base exception for metric method related errors
MethodRegistrationError : Raised when method registration fails
InvalidMethodSignatureError : Raised for invalid method signatures
MetricsMethodNotFoundError : Raised when requested method not found
MethodNotFoundError : Raised when method lookup fails
RegistryLockError : Raised when registry operations fail due to locking
EmptyRegistryError : Raised when operations require non-empty registry
MetricSpecificationError : Raised for invalid metric specifications
"""

import difflib
from typing import Optional

from nhs_herbot import LoggedException


class MetricMethodError(LoggedException):
    """Base exception for all metric method related errors."""


class MethodRegistrationError(MetricMethodError):
    """Exception raised when method registration fails."""

    def __init__(
        self,
        method_name: str,
        reason: str,
        existing_methods: Optional[list[str]] = None,
    ):
        self.method_name = method_name
        self.reason = reason
        self.existing_methods = existing_methods or []

        message = f"Failed to register metric method '{method_name}': {reason}"
        if self.existing_methods:
            available = ", ".join(self.existing_methods)
            message += f". Available methods: {available}"

        super().__init__(message)


class MethodNotFoundError(MetricMethodError):
    """Exception raised when a requested method is not registered."""

    def __init__(self, method_name: str, available_methods: list[str]):
        self.method_name = method_name
        self.available_methods = available_methods

        methods_list = ", ".join(available_methods) if available_methods else "None"
        message = (
            f"Metric method '{method_name}' is not registered. Available methods: {methods_list}"
        )

        if available_methods:
            # Suggest similar method names
            similar = [m for m in available_methods if method_name.lower() in m.lower()]
            if similar:
                similar_list = ", ".join(similar)
                message += f". Did you mean one of: {similar_list}?"

        super().__init__(message)


class MethodExecutionError(MetricMethodError):
    """Exception raised when a metric method fails during execution."""

    def __init__(
        self,
        method_name: str,
        original_error: Exception,
        data_info: Optional[str] = None,
    ):
        self.method_name = method_name
        self.original_error = original_error
        self.data_info = data_info

        message = f"Metric method '{method_name}' failed during execution: {str(original_error)}"
        if data_info:
            message += f". Data info: {data_info}"

        super().__init__(message)


class InvalidMethodSignatureError(MetricMethodError):
    """Exception raised when method has invalid signature for metric use."""

    def __init__(self, method_name: str, signature_issue: str):
        self.method_name = method_name
        self.signature_issue = signature_issue

        message = (
            f"Method '{method_name}' has invalid signature for metric use: "
            f"{signature_issue}. Metric methods should accept at least one "
            f"parameter (the data)."
        )

        super().__init__(message)


class MethodValidationError(MetricMethodError):
    """Exception raised when method validation fails."""

    def __init__(self, method_name: str, validation_issue: str):
        self.method_name = method_name
        self.validation_issue = validation_issue

        message = f"Method '{method_name}' validation failed: {validation_issue}"
        super().__init__(message)


class DuplicateMethodWarning(LoggedException):
    """Warning raised when a method is registered multiple times."""

    def __init__(self, method_name: str, source_info: Optional[str] = None):
        self.method_name = method_name
        self.source_info = source_info

        message = (
            f"Method '{method_name}' is being re-registered, overwriting previous registration"
        )
        if source_info:
            message += f" from {source_info}"

        super().__init__(message)


class RegistryLockError(MetricMethodError):
    """Exception raised when registry operations fail due to threading."""

    def __init__(self, operation: str, reason: str):
        self.operation = operation
        self.reason = reason

        message = f"Registry {operation} failed due to threading issue: {reason}"
        super().__init__(message)


class EmptyRegistryError(MetricMethodError):
    """Exception raised when attempting operations on empty registry."""

    def __init__(self, operation: str):
        self.operation = operation

        message = (
            f"Cannot perform {operation} on empty method registry. "
            f"Register some methods first using @metric_method decorator."
        )

        super().__init__(message)


class MetricSpecificationError(ValueError, LoggedException):
    """Exception raised when metric specification is invalid."""

    def __init__(self, specification_issue: str, method_spec=None):
        self.specification_issue = specification_issue
        self.method_spec = method_spec

        message = f"Invalid metric specification: {specification_issue}"
        if method_spec:
            message += f". Method specification: {method_spec}"

        super().__init__(message)


class MetricsMethodNotFoundError(MetricMethodError):
    """Exception raised when a specified method is not found in metrics methods."""

    def __init__(self, method_name: str, available_methods: list[str]):
        self.method_name = method_name
        self.available_methods = available_methods

        methods_list = ", ".join(available_methods) if available_methods else "None"
        message = (
            f"Metric method '{method_name}' is not registered. Available methods: {methods_list}"
        )

        if available_methods:
            # Use difflib to suggest similar method names
            close_matches = difflib.get_close_matches(
                method_name, available_methods, n=3, cutoff=0.4
            )
            if close_matches:
                suggestions = ", ".join(close_matches)
                message += f". Did you mean one of: {suggestions}?"

        super().__init__(message)

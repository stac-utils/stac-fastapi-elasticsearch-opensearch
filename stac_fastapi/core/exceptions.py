"""Core exceptions module for STAC FastAPI application.

This module contains custom exception classes to handle specific error conditions in a structured way.
"""


class BulkInsertError(Exception):
    """Exception raised for bulk insert operation failures.

    Attributes:
        success_count (int): Number of successfully inserted items
        errors (List[Dict]): Detailed error information for failed operations
        failure_count (int): Derived count of failed operations

    Notes:
        Raised by bulk_async/bulk_sync methods when raise_errors=True
        and any operations fail during bulk insertion.
    """

    def __init__(self, message, success_count, errors):
        """Initialize BulkInsertError instance with operation details.

        Args:
            message (str): Human-readable error description
            success_count (int): Number of successfully processed items
            errors (List[Dict]): List of error dictionaries from bulk operation
        """
        super().__init__(message)
        self.success_count = success_count
        self.errors = errors
        self.failure_count = len(errors)

    def __str__(self) -> str:
        """Return enhanced string representation with operation metrics.

        Returns:
            str: Formatted string containing base message with success/failure counts
        """
        return f"{super().__str__()} (Success: {self.success_count}, Failures: {self.failure_count})"

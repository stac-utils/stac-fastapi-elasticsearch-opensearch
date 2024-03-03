"""Base settings."""

from abc import ABC, abstractmethod


class ApiBaseSettings(ABC):
    """Abstract base class for API settings."""

    @abstractmethod
    def create_client(self):
        """Create a database client."""
        pass

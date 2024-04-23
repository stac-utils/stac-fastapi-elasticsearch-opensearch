"""database session management."""

import logging

import attr

logger = logging.getLogger(__name__)


@attr.s
class Session:
    """Database session management."""

    @classmethod
    def create_from_env(cls):
        """Create from environment."""
        ...

    @classmethod
    def create_from_settings(cls, settings):
        """Create a Session object from settings."""
        ...

    def __attrs_post_init__(self):
        """Post init handler."""
        ...

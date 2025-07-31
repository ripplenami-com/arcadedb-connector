"""
Custom exceptions for ArcadeDB Connector.
"""

from typing import Optional, Any, Dict


class ArcadeDBError(Exception):
    """Base exception for all ArcadeDB related errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details or {}


class ArcadeDBConnectionError(ArcadeDBError):
    """Raised when connection to ArcadeDB fails."""
    pass


class ArcadeDBAuthenticationError(ArcadeDBError):
    """Raised when authentication with ArcadeDB fails."""
    pass


class ArcadeDBQueryError(ArcadeDBError):
    """Raised when a query execution fails."""
    pass


class ArcadeDBTimeoutError(ArcadeDBError):
    """Raised when a request times out."""
    pass


class ArcadeDBConfigurationError(ArcadeDBError):
    """Raised when there's an issue with configuration."""
    pass

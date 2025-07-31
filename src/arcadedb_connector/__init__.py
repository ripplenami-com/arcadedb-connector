"""
ArcadeDB Connector Package

A professional Python connector for ArcadeDB with configuration management,
robust error handling, and comprehensive logging.
"""

from .client import ArcadeDBClient
from .config import ArcadeDBConfig
from .exceptions import (
    ArcadeDBError,
    ArcadeDBConnectionError,
    ArcadeDBAuthenticationError,
    ArcadeDBQueryError,
    ArcadeDBTimeoutError
)

__version__ = "0.1.0"
__all__ = [
    "ArcadeDBClient",
    "ArcadeDBConfig", 
    "ArcadeDBError",
    "ArcadeDBConnectionError",
    "ArcadeDBAuthenticationError",
    "ArcadeDBQueryError",
    "ArcadeDBTimeoutError"
]

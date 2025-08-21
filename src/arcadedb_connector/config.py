"""
Configuration management for ArcadeDB Connector.
"""

import os
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from dotenv import load_dotenv

from .exceptions import ArcadeDBConfigurationError


class ArcadeDBConfig(BaseModel):
    """Configuration class for ArcadeDB connection settings."""
    
    host: str = Field(default="localhost", description="ArcadeDB server host")
    port: int = Field(default=2480, ge=1, le=65535, description="ArcadeDB server port")
    database: str = Field(..., min_length=1, description="Database name")
    username: str = Field(..., min_length=1, description="Username for authentication")
    password: str = Field(..., min_length=1, description="Password for authentication")
    use_ssl: bool = Field(default=False, description="Whether to use SSL/HTTPS")
    timeout: int = Field(default=30, ge=1, description="Request timeout in seconds")
    max_retries: int = Field(default=3, ge=0, description="Maximum number of retry attempts")
    
    @field_validator('host')
    @classmethod
    def validate_host(cls, v: str) -> str:
        """Validate host format."""
        if not v or v.isspace():
            raise ValueError("Host cannot be empty or whitespace")
        return v.strip()
    
    @property
    def base_url(self) -> str:
        """Generate the base URL for ArcadeDB API."""
        protocol = "https" if self.use_ssl else "http"
        return f"{protocol}://{self.host}:{self.port}"
    
    @property
    def api_url(self) -> str:
        """Generate the API URL for database operations."""
        return f"{self.base_url}/api/v1"
    
    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "ArcadeDBConfig":
        """
        Create configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file
            
        Returns:
            ArcadeDBConfig instance
            
        Raises:
            ArcadeDBConfigurationError: If required environment variables are missing
        """
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        required_vars = {
            "ARCADEDB_HOST": "localhost",
            "ARCADEDB_NAME": "database",
            "ARCADEDB_USER": "username",
            "ARCADEDB_PASS": "password"
        }
        
        missing_vars = []
        config_data = {}
        
        # Check required variables
        for env_var, config_key in required_vars.items():
            value = os.getenv(env_var)
            if not value:
                missing_vars.append(env_var)
            else:
                config_data[config_key] = value
        
        if missing_vars:
            raise ArcadeDBConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
        
        # Optional variables with defaults
        optional_vars = {
            "ARCADEDB_HOST": ("host", "localhost"),
            "ARCADEDB_PORT": ("port", 2480),
            "ARCADEDB_USE_SSL": ("use_ssl", False),
            "ARCADEDB_TIMEOUT": ("timeout", 30),
            "ARCADEDB_MAX_RETRIES": ("max_retries", 3)
        }
        
        for env_var, (config_key, default_value) in optional_vars.items():
            value = os.getenv(env_var)
            if value:
                # Type conversion based on default value type
                if isinstance(default_value, bool):
                    config_data[config_key] = value.lower() in ('true', '1', 'yes', 'on')
                elif isinstance(default_value, int):
                    try:
                        config_data[config_key] = int(value)
                    except ValueError:
                        raise ArcadeDBConfigurationError(
                            f"Invalid integer value for {env_var}: {value}"
                        )
                else:
                    config_data[config_key] = value
            else:
                config_data[config_key] = default_value
        
        try:
            return cls(**config_data)
        except Exception as e:
            raise ArcadeDBConfigurationError(f"Configuration validation failed: {str(e)}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary, excluding sensitive data."""
        data = self.model_dump()
        # Don't expose password in logs/debug output
        data["password"] = "***"
        return data

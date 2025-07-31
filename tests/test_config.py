"""
Test configuration management.
"""

import os
import pytest
from unittest.mock import patch, mock_open
from arcadedb_connector.config import ArcadeDBConfig
from arcadedb_connector.exceptions import ArcadeDBConfigurationError


class TestArcadeDBConfig:
    """Test cases for ArcadeDBConfig."""
    
    def test_config_creation_with_required_fields(self):
        """Test creating config with minimum required fields."""
        config = ArcadeDBConfig(
            database="testdb",
            username="testuser", 
            password="testpass"
        )
        
        assert config.database == "testdb"
        assert config.username == "testuser"
        assert config.password == "testpass"
        assert config.host == "localhost"
        assert config.port == 2480
        assert config.use_ssl is False
        assert config.timeout == 30
        assert config.max_retries == 3
    
    def test_config_validation_empty_database(self):
        """Test validation fails with empty database name."""
        with pytest.raises(ValueError):
            ArcadeDBConfig(
                database="",
                username="testuser",
                password="testpass"
            )
    
    def test_config_validation_invalid_port(self):
        """Test validation fails with invalid port."""
        with pytest.raises(ValueError):
            ArcadeDBConfig(
                database="testdb",
                username="testuser",
                password="testpass",
                port=70000  # Invalid port
            )
    
    def test_base_url_generation(self):
        """Test base URL generation."""
        config = ArcadeDBConfig(
            database="testdb",
            username="testuser",
            password="testpass",
            host="example.com",
            port=8080,
            use_ssl=True
        )
        
        assert config.base_url == "https://example.com:8080"
    
    def test_api_url_generation(self):
        """Test API URL generation."""
        config = ArcadeDBConfig(
            database="testdb",
            username="testuser",
            password="testpass"
        )
        
        assert config.api_url == "http://localhost:2480/api/v1"
    
    def test_to_dict_masks_password(self):
        """Test that to_dict masks the password."""
        config = ArcadeDBConfig(
            database="testdb",
            username="testuser",
            password="secretpass"
        )
        
        config_dict = config.to_dict()
        assert config_dict["password"] == "***"
        assert config_dict["username"] == "testuser"
    
    @patch.dict(os.environ, {
        'ARCADEDB_DATABASE': 'envdb',
        'ARCADEDB_USERNAME': 'envuser',
        'ARCADEDB_PASSWORD': 'envpass',
        'ARCADEDB_HOST': 'envhost',
        'ARCADEDB_PORT': '3000',
        'ARCADEDB_USE_SSL': 'true'
    })
    def test_from_env_success(self):
        """Test creating config from environment variables."""
        config = ArcadeDBConfig.from_env()
        
        assert config.database == "envdb"
        assert config.username == "envuser"
        assert config.password == "envpass"
        assert config.host == "envhost"
        assert config.port == 3000
        assert config.use_ssl is True
    
    @patch.dict(os.environ, {
        'ARCADEDB_DATABASE': 'envdb',
        'ARCADEDB_USERNAME': 'envuser'
        # Missing password
    }, clear=True)
    def test_from_env_missing_required(self):
        """Test from_env fails with missing required variables."""
        with pytest.raises(ArcadeDBConfigurationError) as exc_info:
            ArcadeDBConfig.from_env()
        
        assert "ARCADEDB_PASSWORD" in str(exc_info.value)
    
    @patch.dict(os.environ, {
        'ARCADEDB_DATABASE': 'envdb',
        'ARCADEDB_USERNAME': 'envuser',
        'ARCADEDB_PASSWORD': 'envpass',
        'ARCADEDB_PORT': 'invalid_port'
    })
    def test_from_env_invalid_port(self):
        """Test from_env fails with invalid port."""
        with pytest.raises(ArcadeDBConfigurationError):
            ArcadeDBConfig.from_env()
    
    def test_host_validation_empty(self):
        """Test host validation with empty string."""
        with pytest.raises(ValueError):
            ArcadeDBConfig(
                database="testdb",
                username="testuser",
                password="testpass",
                host=""
            )
    
    def test_host_validation_whitespace(self):
        """Test host validation with whitespace."""
        config = ArcadeDBConfig(
            database="testdb",
            username="testuser",
            password="testpass",
            host="  localhost  "
        )
        
        assert config.host == "localhost"

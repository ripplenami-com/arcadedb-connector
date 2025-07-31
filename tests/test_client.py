"""
Test ArcadeDB client functionality.
"""

import json
from unittest.mock import Mock, patch, MagicMock
import pytest
import requests

from arcadedb_connector.client import ArcadeDBClient
from arcadedb_connector.config import ArcadeDBConfig
from arcadedb_connector.exceptions import (
    ArcadeDBConnectionError,
    ArcadeDBAuthenticationError,
    ArcadeDBTimeoutError,
    ArcadeDBError
)


@pytest.fixture
def config():
    """Create test configuration."""
    return ArcadeDBConfig(
        database="testdb",
        username="testuser",
        password="testpass",
        host="localhost",
        port=2480
    )


@pytest.fixture
def client(config):
    """Create test client."""
    return ArcadeDBClient(config)


class TestArcadeDBClient:
    """Test cases for ArcadeDBClient."""
    
    def test_client_initialization(self, config):
        """Test client initialization."""
        client = ArcadeDBClient(config)
        
        assert client.config == config
        assert client.session is not None
        assert client._authenticated is False
        assert client.logger is not None
    
    @patch('requests.Session.request')
    def test_connect_success(self, mock_request, client):
        """Test successful connection."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        result = client.connect()
        
        assert result is True
        mock_request.assert_called_once()
    
    @patch('requests.Session.request')
    def test_connect_failure(self, mock_request, client):
        """Test connection failure."""
        mock_request.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with pytest.raises(ArcadeDBConnectionError):
            client.connect()
    
    @patch('requests.Session.request')
    def test_authenticate_success(self, mock_request, client):
        """Test successful authentication."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        mock_request.return_value = mock_response
        
        result = client.authenticate()
        
        assert result is True
        assert client._authenticated is True
    
    @patch('requests.Session.request')
    def test_authenticate_failure(self, mock_request, client):
        """Test authentication failure."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_request.return_value = mock_response
        
        with pytest.raises(ArcadeDBAuthenticationError):
            client.authenticate()
        
        assert client._authenticated is False
    
    @patch('requests.Session.request')
    def test_execute_query_success(self, mock_request, client):
        """Test successful query execution."""
        # Mock authentication call
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock query call
        query_response = Mock()
        query_response.status_code = 200
        query_response.json.return_value = {"result": [{"name": "test"}]}
        
        mock_request.side_effect = [auth_response, query_response]
        
        result = client.execute_query("SELECT * FROM Person")
        
        assert "result" in result
        assert len(result["result"]) == 1
    
    @patch('requests.Session.request')
    def test_execute_query_with_parameters(self, mock_request, client):
        """Test query execution with parameters."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock query
        query_response = Mock()
        query_response.status_code = 200
        query_response.json.return_value = {"result": []}
        
        mock_request.side_effect = [auth_response, query_response]
        
        parameters = {"min_age": 25}
        result = client.execute_query(
            "SELECT * FROM Person WHERE age > :min_age", 
            parameters
        )
        
        # Check that parameters were included in the request
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        assert call_args[1]['json']['parameters'] == parameters
    
    @patch('requests.Session.request')
    def test_create_document_success(self, mock_request, client):
        """Test successful document creation."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock document creation
        doc_response = Mock()
        doc_response.status_code = 200
        doc_response.json.return_value = {"@rid": "#1:0", "name": "test"}
        
        mock_request.side_effect = [auth_response, doc_response]
        
        document = {"name": "test", "email": "test@example.com"}
        result = client.create_document("Person", document)
        
        assert "@rid" in result
        assert result["name"] == "test"
    
    @patch('requests.Session.request')
    def test_get_document_success(self, mock_request, client):
        """Test successful document retrieval."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock document retrieval
        doc_response = Mock()
        doc_response.status_code = 200
        doc_response.json.return_value = {"@rid": "#1:0", "name": "test"}
        
        mock_request.side_effect = [auth_response, doc_response]
        
        result = client.get_document("#1:0")
        
        assert result["@rid"] == "#1:0"
        assert result["name"] == "test"
    
    @patch('requests.Session.request')
    def test_timeout_error(self, mock_request, client):
        """Test timeout error handling."""
        mock_request.side_effect = requests.exceptions.Timeout()
        
        with pytest.raises(ArcadeDBTimeoutError):
            client.connect()
    
    @patch('requests.Session.request')
    def test_http_error_handling(self, mock_request, client):
        """Test HTTP error handling."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.reason = "Internal Server Error"
        mock_response.json.return_value = {"error": "Database error"}
        mock_request.return_value = mock_response
        
        with pytest.raises(ArcadeDBError) as exc_info:
            client.connect()
        
        assert exc_info.value.status_code == 500
    
    def test_context_manager(self, config):
        """Test client as context manager."""
        with patch.object(ArcadeDBClient, 'close') as mock_close:
            with ArcadeDBClient(config) as client:
                assert isinstance(client, ArcadeDBClient)
            
            mock_close.assert_called_once()
    
    @patch('requests.Session.request')
    def test_get_server_info(self, mock_request, client):
        """Test getting server information."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"version": "24.1.1"}
        mock_request.return_value = mock_response
        
        result = client.get_server_info()
        
        assert result["version"] == "24.1.1"
    
    @patch('requests.Session.request')
    def test_list_databases(self, mock_request, client):
        """Test listing databases."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": [
                {"name": "db1"},
                {"name": "db2"}
            ]
        }
        mock_request.return_value = mock_response
        
        result = client.list_databases()
        
        assert result == ["db1", "db2"]

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
    with patch('arcadedb_connector.client.ArcadeDBClient.connect'):
        return ArcadeDBClient(config)


class TestArcadeDBClient:
    """Test cases for ArcadeDBClient."""
    
    def test_client_initialization(self, config):
        """Test client initialization."""
        with patch('arcadedb_connector.client.ArcadeDBClient.connect'):
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

    @patch('requests.Session.request')
    def test_list_databases_empty_result(self, mock_request, client):
        """Test listing databases with empty result."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response
        
        result = client.list_databases()
        
        assert result == []

    @patch('requests.Session.request')
    def test_list_classes_success(self, mock_request, client):
        """Test listing classes successfully."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock list classes
        classes_response = Mock()
        classes_response.status_code = 200
        classes_response.json.return_value = {
            "result": [
                {"name": "Person"},
                {"name": "Company"},
                {"name": "Document"}
            ]
        }
        
        mock_request.side_effect = [auth_response, classes_response]
        
        result = client.list_classes()
        
        assert result == ["Person", "Company", "Document"]
        
        # Verify the SQL query was correct
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        assert call_args[1]['json']['command'] == "SELECT FROM schema:types"

    @patch('requests.Session.request')
    def test_list_classes_empty_result(self, mock_request, client):
        """Test listing classes with empty result."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock empty classes response
        classes_response = Mock()
        classes_response.status_code = 200
        classes_response.json.return_value = {}
        
        mock_request.side_effect = [auth_response, classes_response]
        
        result = client.list_classes()
        
        assert result == []

    @patch('requests.Session.request')
    def test_list_classes_failure(self, mock_request, client):
        """Test list classes failure."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock failed classes request
        mock_request.side_effect = [auth_response, requests.exceptions.ConnectionError("Connection failed")]
        
        with pytest.raises(ArcadeDBError):
            client.list_classes()

    @patch('requests.Session.request')
    def test_count_values_schema_basic(self, mock_request, client):
        """Test count_values_schema with basic parameters."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock count query
        count_response = Mock()
        count_response.status_code = 200
        count_response.json.return_value = {
            "result": [{"counting": 42}]
        }
        
        mock_request.side_effect = [auth_response, count_response]
        
        result = client.count_values_schema("Person")
        
        assert result == 42
        
        # Verify the SQL query was correct
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        expected_query = "SELECT COUNT(*) AS counting from `Person`"
        assert call_args[1]['json']['command'] == expected_query

    @patch('requests.Session.request')
    def test_count_values_schema_with_customer_type(self, mock_request, client):
        """Test count_values_schema with customer_type_id."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock count query
        count_response = Mock()
        count_response.status_code = 200
        count_response.json.return_value = {
            "result": [{"counting": 15}]
        }
        
        mock_request.side_effect = [auth_response, count_response]
        
        result = client.count_values_schema("Person", customer_type_id=123)
        
        assert result == 15
        
        # Verify the SQL query was correct
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        expected_query = "SELECT COUNT(*) AS counting from `Person` where CustomerTypeId = 123 "
        assert call_args[1]['json']['command'] == expected_query

    @patch('requests.Session.request')
    def test_count_values_schema_with_not_null(self, mock_request, client):
        """Test count_values_schema with is_not_null parameter."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock count query
        count_response = Mock()
        count_response.status_code = 200
        count_response.json.return_value = {
            "result": [{"counting": 30}]
        }
        
        mock_request.side_effect = [auth_response, count_response]
        
        result = client.count_values_schema("Person", is_not_null="email")
        
        assert result == 30
        
        # Verify the SQL query was correct
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        expected_query = "SELECT COUNT(*) AS counting from `Person` WHERE email IS NOT NULL"
        assert call_args[1]['json']['command'] == expected_query

    @patch('requests.Session.request')
    def test_count_values_schema_with_both_params(self, mock_request, client):
        """Test count_values_schema with both customer_type_id and is_not_null."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock count query
        count_response = Mock()
        count_response.status_code = 200
        count_response.json.return_value = {
            "result": [{"counting": 8}]
        }
        
        mock_request.side_effect = [auth_response, count_response]
        
        result = client.count_values_schema("Person", customer_type_id=456, is_not_null="phone")
        
        assert result == 8
        
        # Verify the SQL query was correct
        call_args = mock_request.call_args_list[1]
        assert 'json' in call_args[1]
        expected_query = "SELECT COUNT(*) AS counting from `Person` where CustomerTypeId = 456 and phone IS NOT NULL "
        assert call_args[1]['json']['command'] == expected_query

    @patch('requests.Session.request')
    def test_count_values_schema_failure(self, mock_request, client):
        """Test count_values_schema failure."""
        # Mock authentication
        auth_response = Mock()
        auth_response.status_code = 200
        auth_response.json.return_value = {"result": "success"}
        
        # Mock failed count request
        mock_request.side_effect = [auth_response, requests.exceptions.ConnectionError("Connection failed")]
        
        with pytest.raises(ArcadeDBError):
            client.count_values_schema("Person")

    def test_setup_logger(self, client):
        """Test logger setup."""
        logger = client._setup_logger()
        
        assert logger is not None
        assert logger.name.endswith("ArcadeDBClient")
        assert len(logger.handlers) > 0

    def test_setup_session(self, client):
        """Test session setup."""
        session = client._setup_session()
        
        assert isinstance(session, requests.Session)
        assert 'Content-Type' in session.headers
        assert session.headers['Content-Type'] == 'application/json'
        assert 'Accept' in session.headers
        assert session.headers['Accept'] == 'application/json'
        assert 'User-Agent' in session.headers
        assert 'arcadedb-connector' in str(session.headers['User-Agent'])

    @patch('requests.Session.request')
    def test_make_request_basic(self, mock_request, client):
        """Test basic _make_request functionality."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}
        mock_request.return_value = mock_response
        
        response = client._make_request('GET', 'test-endpoint')
        
        assert response.status_code == 200
        mock_request.assert_called_once()

    @patch('requests.Session.request')
    def test_make_request_with_data(self, mock_request, client):
        """Test _make_request with data payload."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        test_data = {"key": "value"}
        client._make_request('POST', 'test-endpoint', data=test_data)
        
        call_args = mock_request.call_args
        assert 'json' in call_args[1]
        assert call_args[1]['json'] == test_data

    @patch('requests.Session.request')
    def test_make_request_with_params(self, mock_request, client):
        """Test _make_request with query parameters."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        test_params = {"param1": "value1", "param2": "value2"}
        client._make_request('GET', 'test-endpoint', params=test_params)
        
        call_args = mock_request.call_args
        assert 'params' in call_args[1]
        assert call_args[1]['params'] == test_params

    @patch('requests.Session.request')
    def test_make_request_no_auth(self, mock_request, client):
        """Test _make_request without authentication."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        client._make_request('GET', 'test-endpoint', authenticate=False)
        
        call_args = mock_request.call_args
        assert 'auth' not in call_args[1]

    @patch('requests.Session.request')
    def test_make_request_404_error(self, mock_request, client):
        """Test _make_request with 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response
        
        with pytest.raises(ArcadeDBError) as exc_info:
            client._make_request('GET', 'nonexistent-endpoint')
        
        assert exc_info.value.status_code == 404
        assert "Resource not found" in str(exc_info.value)

    @patch('requests.Session.request')
    def test_make_request_with_error_response_json(self, mock_request, client):
        """Test _make_request with error response containing JSON."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.reason = "Bad Request"
        mock_response.json.return_value = {"error": "Invalid query syntax"}
        mock_request.return_value = mock_response
        
        with pytest.raises(ArcadeDBError) as exc_info:
            client._make_request('POST', 'test-endpoint')
        
        assert "Invalid query syntax" in str(exc_info.value)
        assert exc_info.value.status_code == 400

    @patch('requests.Session.request')
    def test_make_request_connection_error(self, mock_request, client):
        """Test _make_request with connection error."""
        mock_request.side_effect = requests.exceptions.ConnectionError("Network unreachable")
        
        with pytest.raises(ArcadeDBConnectionError):
            client._make_request('GET', 'test-endpoint')

    @patch('requests.Session.request')
    def test_make_request_request_exception(self, mock_request, client):
        """Test _make_request with general request exception."""
        mock_request.side_effect = requests.exceptions.RequestException("General error")
        
        with pytest.raises(ArcadeDBError) as exc_info:
            client._make_request('GET', 'test-endpoint')
        
        assert "Request failed" in str(exc_info.value)

    def test_context_manager_with_patch(self, config):
        """Test client as context manager with proper mocking."""
        with patch('arcadedb_connector.client.ArcadeDBClient.connect'), \
             patch.object(ArcadeDBClient, 'close') as mock_close:
            with ArcadeDBClient(config) as client:
                assert isinstance(client, ArcadeDBClient)
            
            mock_close.assert_called_once()

    def test_close_method(self, client):
        """Test close method."""
        # Mock the session
        mock_session = Mock()
        client.session = mock_session
        
        client.close()
        
        mock_session.close.assert_called_once()

    @patch('requests.Session.request')
    def test_get_server_info_without_auth(self, mock_request, client):
        """Test getting server info without authentication."""
        client._authenticated = False
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"version": "24.1.1", "user": "anonymous"}
        mock_request.return_value = mock_response
        
        result = client.get_server_info()
        
        assert result["version"] == "24.1.1"
        # Verify it was called without authentication
        call_args = mock_request.call_args
        assert call_args[1].get('auth') is None

    @patch('requests.Session.request')
    def test_get_server_info_with_auth(self, mock_request, client):
        """Test getting server info with authentication."""
        client._authenticated = True
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"version": "24.1.1", "user": "testuser"}
        mock_request.return_value = mock_response
        
        result = client.get_server_info()
        
        assert result["version"] == "24.1.1"
        # Verify it was called with authentication
        call_args = mock_request.call_args
        assert call_args[1].get('auth') == (client.config.username, client.config.password)

"""
Test utility functions.
"""

from datetime import datetime
from arcadedb_connector.utils import (
    validate_rid,
    format_query_parameters,
    build_where_clause,
    sanitize_identifier,
    parse_error_response
)


class TestUtils:
    """Test cases for utility functions."""
    
    def test_validate_rid_valid(self):
        """Test RID validation with valid RIDs."""
        assert validate_rid("#1:0") is True
        assert validate_rid("#123:456") is True
        assert validate_rid("#0:0") is True
    
    def test_validate_rid_invalid(self):
        """Test RID validation with invalid RIDs."""
        assert validate_rid("1:0") is False  # Missing #
        assert validate_rid("#1") is False   # Missing position
        assert validate_rid("#:0") is False  # Missing bucket
        assert validate_rid("#a:0") is False # Non-numeric bucket
        assert validate_rid("#1:b") is False # Non-numeric position
        assert validate_rid("") is False     # Empty string
    
    def test_format_query_parameters_datetime(self):
        """Test formatting datetime parameters."""
        dt = datetime(2025, 1, 1, 12, 0, 0)
        params = {"created_at": dt}
        
        result = format_query_parameters(params)
        
        assert "created_at" in result
        assert result["created_at"] == dt.isoformat()
    
    def test_format_query_parameters_mixed(self):
        """Test formatting mixed parameter types."""
        params = {
            "name": "John",
            "age": 30,
            "active": True,
            "tags": ["python", "developer"],
            "metadata": {"level": "senior"}
        }
        
        result = format_query_parameters(params)
        
        assert result["name"] == "John"
        assert result["age"] == 30
        assert result["active"] is True
        assert result["tags"] == ["python", "developer"]
        assert result["metadata"] == {"level": "senior"}
    
    def test_build_where_clause_empty(self):
        """Test building WHERE clause with empty conditions."""
        result = build_where_clause({})
        assert result == ""
    
    def test_build_where_clause_single_condition(self):
        """Test building WHERE clause with single condition."""
        conditions = {"name": "John"}
        result = build_where_clause(conditions)
        assert result == "WHERE name = 'John'"
    
    def test_build_where_clause_multiple_conditions(self):
        """Test building WHERE clause with multiple conditions."""
        conditions = {
            "name": "John",
            "age": 30,
            "active": True,
            "score": None
        }
        result = build_where_clause(conditions)
        
        assert "WHERE" in result
        assert "name = 'John'" in result
        assert "age = 30" in result
        assert "active = true" in result
        assert "score IS NULL" in result
        assert " AND " in result
    
    def test_sanitize_identifier_valid(self):
        """Test sanitizing valid identifiers."""
        assert sanitize_identifier("valid_name") == "valid_name"
        assert sanitize_identifier("ValidName123") == "ValidName123"
    
    def test_sanitize_identifier_invalid_chars(self):
        """Test sanitizing identifiers with invalid characters."""
        assert sanitize_identifier("invalid-name") == "invalid_name"
        assert sanitize_identifier("invalid.name") == "invalid_name"
        assert sanitize_identifier("invalid@name") == "invalid_name"
        assert sanitize_identifier("invalid name") == "invalid_name"
    
    def test_sanitize_identifier_starts_with_number(self):
        """Test sanitizing identifiers that start with numbers."""
        assert sanitize_identifier("123name") == "_123name"
        assert sanitize_identifier("1invalid") == "_1invalid"
    
    def test_sanitize_identifier_empty(self):
        """Test sanitizing empty identifier."""
        assert sanitize_identifier("") == "unnamed"
        assert sanitize_identifier("   ") == "unnamed"
    
    def test_parse_error_response_with_error(self):
        """Test parsing error response with 'error' field."""
        response = {"error": "Database connection failed"}
        result = parse_error_response(response)
        assert result == "Database connection failed"
    
    def test_parse_error_response_with_exception(self):
        """Test parsing error response with 'exception' field."""
        response = {"exception": "NullPointerException"}
        result = parse_error_response(response)
        assert result == "NullPointerException"
    
    def test_parse_error_response_with_message(self):
        """Test parsing error response with 'message' field."""
        response = {"message": "Query syntax error"}
        result = parse_error_response(response)
        assert result == "Query syntax error"
    
    def test_parse_error_response_fallback(self):
        """Test parsing error response fallback."""
        response = {"unexpected": "data"}
        result = parse_error_response(response)
        assert result == str(response)

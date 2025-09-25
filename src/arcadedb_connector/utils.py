"""
Utility functions for ArcadeDB Connector.
"""

import re
import os
from typing import Dict, Any, List
from datetime import datetime
from constants import INDEX_COLUMNS


def validate_rid(rid: str) -> bool:
    """
    Validate ArcadeDB Record ID format.
    
    Args:
        rid: Record ID to validate
        
    Returns:
        True if valid RID format
    """
    # ArcadeDB RID format: #<bucket_id>:<position>
    pattern = r'^#\d+:\d+$'
    return bool(re.match(pattern, rid))


def format_query_parameters(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format query parameters for ArcadeDB.
    
    Args:
        params: Raw parameters dictionary
        
    Returns:
        Formatted parameters dictionary
    """
    formatted = {}
    
    for key, value in params.items():
        if isinstance(value, datetime):
            # Convert datetime to ISO format
            formatted[key] = value.isoformat()
        elif isinstance(value, (list, dict)):
            # Keep complex types as-is (ArcadeDB handles JSON)
            formatted[key] = value
        else:
            formatted[key] = value
    
    return formatted


def build_where_clause(conditions: Dict[str, Any]) -> str:
    """
    Build WHERE clause from conditions dictionary.
    
    Args:
        conditions: Dictionary of field->value conditions
        
    Returns:
        WHERE clause string
    """
    if not conditions:
        return ""
    
    clauses = []
    for field, value in conditions.items():
        if isinstance(value, str):
            clauses.append(f"{field} = '{value}'")
        elif isinstance(value, (int, float)):
            clauses.append(f"{field} = {value}")
        elif isinstance(value, bool):
            clauses.append(f"{field} = {str(value).lower()}")
        elif value is None:
            clauses.append(f"{field} IS NULL")
        else:
            # For complex values, use parameter binding
            clauses.append(f"{field} = :{field}")
    
    return "WHERE " + " AND ".join(clauses)


def sanitize_identifier(identifier: str) -> str:
    """
    Sanitize database identifier (bucket name, field name, etc.).
    
    Args:
        identifier: Raw identifier
        
    Returns:
        Sanitized identifier
    """
    # Remove special characters and replace with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', identifier)
    
    # Ensure it starts with a letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    
    return sanitized or "unnamed"


def parse_error_response(response_data: Dict[str, Any]) -> str:
    """
    Parse error response from ArcadeDB and extract meaningful error message.
    
    Args:
        response_data: Error response dictionary
        
    Returns:
        Formatted error message
    """
    if 'error' in response_data:
        return str(response_data['error'])
    
    if 'exception' in response_data:
        return str(response_data['exception'])
    
    if 'message' in response_data:
        return str(response_data['message'])
    
    # Fallback to string representation
    return str(response_data)

def read_file_content(file_path: str) -> str:
    """
    Read the content of a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Content of the file as a string
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"JSON File With columns is not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()
    

def format_columns(columns: List[Dict[str, Any]]) -> List[str]:
    """
    Format columns for ArcadeDB insert operation.
    
    Args:
        columns: List of column definitions
        
    Returns:
        List of formatted column names
    """
    formatted_columns = []
    
    for col in columns:
        if 'name' in col:
            name = sanitize_identifier(col['name'])
            formatted_columns.append(name)
        else:
            raise ValueError("Column definition must contain 'name' key")
    
    return formatted_columns

def get_column_names_from_df(df) -> List[Dict[str, Any]]:
    """
    Extract and sanitize column names from a DataFrame.
    
    Args:
        df: DataFrame object

    Returns:
        List of sanitized column names
    """
    if df.empty:
        return []
    
    columns = df.columns.tolist()
    new_columns = []

    for i, col in enumerate(columns):
        new_columns.append({
                  "name": col,
                  "type": str(df[col].dtype).upper(),
                  "index": True if col in INDEX_COLUMNS else False
        })

    return new_columns
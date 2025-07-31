"""
Main ArcadeDB client implementation.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import ArcadeDBConfig
from .exceptions import (
    ArcadeDBError,
    ArcadeDBConnectionError,
    ArcadeDBAuthenticationError,
    ArcadeDBQueryError,
    ArcadeDBTimeoutError
)


class ArcadeDBClient:
    """
    Professional ArcadeDB client with robust error handling and connection management.
    """
    
    def __init__(self, config: ArcadeDBConfig):
        """
        Initialize ArcadeDB client.
        
        Args:
            config: ArcadeDB configuration instance
        """
        self.config = config
        self.logger = self._setup_logger()
        self.session = self._setup_session()
        self._authenticated = False
        
        self.logger.info(
            "Initialized ArcadeDB client for %s:%s/%s",
            config.host, config.port, config.database
        )
        self.connect()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for the client."""
        logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _setup_session(self) -> requests.Session:
        """Set up requests session with retry strategy."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': f'arcadedb-connector/0.1.0'
        })
        
        return session
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        authenticate: bool = True
    ) -> requests.Response:
        """
        Make HTTP request to ArcadeDB API.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request payload
            params: Query parameters
            authenticate: Whether to include authentication
            
        Returns:
            Response object
            
        Raises:
            ArcadeDBError: For various API errors
        """
        url = urljoin(self.config.api_url + "/", endpoint.lstrip("/"))
        
        kwargs = {
            'timeout': self.config.timeout,
            'params': params
        }
        
        if data is not None:
            kwargs['json'] = data
        
        if authenticate:
            kwargs['auth'] = (self.config.username, self.config.password)
        
        self.logger.debug("Making %s request to %s", method.upper(), url)
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            # Handle common HTTP errors
            if response.status_code == 401:
                raise ArcadeDBAuthenticationError(
                    "Authentication failed. Check username and password.",
                    status_code=401
                )
            elif response.status_code == 404:
                raise ArcadeDBError(
                    f"Resource not found: {endpoint}",
                    status_code=404
                )
            elif response.status_code >= 400:
                error_msg = f"HTTP {response.status_code}: {response.reason}"
                try:
                    error_data = response.json()
                    if 'error' in error_data:
                        error_msg = error_data['error']
                except (json.JSONDecodeError, KeyError):
                    pass
                
                raise ArcadeDBError(error_msg, status_code=response.status_code)
            
            return response
            
        except requests.exceptions.Timeout:
            raise ArcadeDBTimeoutError(
                f"Request timed out after {self.config.timeout} seconds"
            )
        except requests.exceptions.ConnectionError as e:
            raise ArcadeDBConnectionError(
                f"Failed to connect to ArcadeDB at {self.config.base_url}: {str(e)}"
            )
        except requests.exceptions.RequestException as e:
            raise ArcadeDBError(f"Request failed: {str(e)}")
    
    def connect(self) -> bool:
        """
        Test connection to ArcadeDB server.
        
        Returns:
            True if connection successful
            
        Raises:
            ArcadeDBError: If connection fails
        """
        try:
            response = self._make_request('GET', 'ready', authenticate=False)
            self.logger.info("Successfully connected to ArcadeDB server")
            return True
        except Exception as e:
            self.logger.error("Failed to connect to ArcadeDB: %s", str(e))
            raise
    
    def authenticate(self) -> bool:
        """
        Authenticate with ArcadeDB server.
        
        Returns:
            True if authentication successful
            
        Raises:
            ArcadeDBAuthenticationError: If authentication fails
        """
        try:
            response = self._make_request('GET', f'/exists/{self.config.database}')
            self._authenticated = True
            self.logger.info("Successfully authenticated with ArcadeDB")
            return True
        except ArcadeDBAuthenticationError:
            self._authenticated = False
            self.logger.error("Authentication failed")
            raise
        except Exception as e:
            self._authenticated = False
            self.logger.error("Authentication error: %s", str(e))
            raise ArcadeDBAuthenticationError(f"Authentication failed: {str(e)}")
    
    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute SQL query on ArcadeDB.
        
        Args:
            query: SQL query string
            parameters: Query parameters
            
        Returns:
            Query result as dictionary
            
        Raises:
            ArcadeDBQueryError: If query execution fails
        """
        if not self._authenticated:
            self.authenticate()
        
        payload: Dict[str, Any] = {
            'command': query,
            'language': 'sql'
        }
        
        if parameters:
            payload['parameters'] = parameters
        
        try:
            response = self._make_request(
                'POST',
                f'command/{self.config.database}',
                data=payload
            )
            
            result = response.json()
            self.logger.debug("Query executed successfully")
            return result
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBQueryError(error_msg)
    
    def create_document(
        self,
        bucket_name: str,
        document: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new document in specified bucket.
        
        Args:
            bucket_name: Name of the bucket/type
            document: Document data
            
        Returns:
            Created document with metadata
            
        Raises:
            ArcadeDBError: If document creation fails
        """
        if not self._authenticated:
            self.authenticate()
        
        try:
            response = self._make_request(
                'POST',
                f'document/{self.config.database}',
                data={
                    '@type': bucket_name,
                    **document
                }
            )
            
            result = response.json()
            self.logger.debug("Document created successfully in bucket %s", bucket_name)
            return result
            
        except Exception as e:
            error_msg = f"Document creation failed: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
    
    def get_document(self, rid: str) -> Dict[str, Any]:
        """
        Retrieve document by RID.
        
        Args:
            rid: Record ID
            
        Returns:
            Document data
            
        Raises:
            ArcadeDBError: If document retrieval fails
        """
        if not self._authenticated:
            self.authenticate()
        
        try:
            response = self._make_request(
                'GET',
                f'document/{self.config.database}/{rid}'
            )
            
            result = response.json()
            self.logger.debug("Document retrieved successfully: %s", rid)
            return result
            
        except Exception as e:
            error_msg = f"Document retrieval failed: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
    
    def get_server_info(self) -> Dict[str, Any]:
        """
        Get ArcadeDB server information.
        
        Returns:
            Server information dictionary
            
        Raises:
            ArcadeDBError: If request fails
        """
        try:
            response = self._make_request('GET', 'server', authenticate=self._authenticated)
            result = response.json()
            self.logger.debug("Server info retrieved successfully")
            return result
            
        except Exception as e:
            error_msg = f"Failed to get server info: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
    
    def list_databases(self) -> List[str]:
        """
        List available databases.
        
        Returns:
            List of database names
            
        Raises:
            ArcadeDBError: If request fails
        """
        try:
            response = self._make_request('GET', 'databases')
            result = response.json()
            
            if isinstance(result, dict) and 'result' in result:
                databases = result['result']
            else:
                databases = []
            
            self.logger.debug("Retrieved %d databases", len(databases))
            return databases
            
        except Exception as e:
            error_msg = f"Failed to list databases: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def list_classes(self) -> List[str]:
        """
        List available classes (buckets/types) in the current database.
        
        Returns:
            List of class names
            
        Raises:
            ArcadeDBError: If request fails
        """
        if not self._authenticated:
            self.authenticate()

        payload = {
            "command": "SELECT FROM schema:types",
            "language": "sql"
        }
        
        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            
            if isinstance(result, dict) and 'result' in result:
                classes = [cls['name'] for cls in result['result']]
            else:
                classes = []
            
            self.logger.debug("Retrieved %d classes", len(classes))
            return classes
            
        except Exception as e:
            error_msg = f"Failed to list classes: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
    
    def count_values_schema(self, schema_name, customer_type_id=None, is_not_null=None) -> int:
        """
        count_values_schema Count the number or records in the schema received as parameter

        :param schema_name: name of the schema in the DB
        :type schema_name: string

        :return: Number of records in the table
        :rtype: integer
        """

        if not self._authenticated:
            self.authenticate()

        if customer_type_id == None:
            if is_not_null == None:
                limitQuery = "SELECT COUNT(*) AS counting from `{}`".format(schema_name)
            else:
                limitQuery = (
                    "SELECT COUNT(*) AS counting from `{}` WHERE {} IS NOT NULL".format(
                        schema_name, is_not_null
                    )
                )
        else:
            if is_not_null == None:
                limitQuery = "SELECT COUNT(*) AS counting from `{}` where CustomerTypeId = {} ".format(
                    schema_name, customer_type_id
                )
            else:
                limitQuery = "SELECT COUNT(*) AS counting from `{}` where CustomerTypeId = {} and {} IS NOT NULL ".format(
                    schema_name, customer_type_id, is_not_null
                )

        payload = {
            "command": limitQuery,
            "language": "sql"
        }

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            return result['result'][0]['counting']

        except Exception as e:
            error_msg = f"Failed to list classes: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def update_counter(self, schema, field_name, value):
        """
        Update a counter field in the specified schema.
        
        Args:
            schema: Name of the schema
            field_name: Field to update
            value: Value to set
        """
        if not self._authenticated:
            self.authenticate()

        payload = {
            "command": f"UPDATE `{schema}` SET {field_name} = {value}",
            "language": "sql"
        }

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            self.logger.debug("Counter updated successfully in schema %s", schema)
            return result
            
        except Exception as e:
            error_msg = f"Failed to update counter: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def close(self) -> None:
        """Close the client session."""
        if self.session:
            self.session.close()
            self.logger.info("ArcadeDB client session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

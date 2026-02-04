"""
Main ArcadeDB client implementation.
"""

import json
import logging
import os
import time
from typing import Dict, Any, Optional, List, Union
from urllib import response
from urllib.parse import urljoin

import requests
import pandas as pd
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import ArcadeDBConfig
from .utils import read_file_content, format_columns, get_column_names_from_df
from .constants import PAGE_SIZE, BATCH_SIZE
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
        try:
            response = self._make_request('GET', 'ready', authenticate=False)
            self.authenticate()
            return True
        except Exception as e:
            self.logger.error("Failed to connect to ArcadeDB: %s", str(e))
            raise
    
    def authenticate(self) -> bool:
        try:
            response = self._make_request('GET', f'/exists/{self.config.database}')
            self._authenticated = True
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
            return result
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBQueryError(error_msg)
    
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
    
    def get_next_version(self, classname, bucket):
        """
        get_next_version Based on the given parameters, detects if a new version is necessary. Reads the `versions` table,
        if a record for the classname with the specific timestam exist, it's not necessary to create a new version.

        :param classname: Name of the class schema (table)
        :type classname: string
        :param timestamp: timestamp of the last change of the file, based on the operating system.
        :type timestamp: string
        :param bucket: name of the source database from UTACS: URA, KCCA, ...
        :type bucket: string
        :return: returns a new version and True (meaning that a new version of the file exists),
        or the last version if the file has been uploaded (and false).
        :rtype: [integer, Boolean]
        """

        lastVersion = 0
        if not self._authenticated:
            self.authenticate()

        sql = "SELECT max(version) as lastversion  FROM versions WHERE classname == '{}' and `bucket` = '{}'".format(
                classname, bucket
            )
        
        payload = {
            "command": sql,
            "language": "sql"
        }
        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            if 'result' not in result or len(result['result']) == 0:
                return 1, True
            lastVersion = result.get('result', [{}])[0].get('lastversion', 0)
            return lastVersion + 1, lastVersion > 0

        except Exception as e:
            error_msg = f"Failed to update counter: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def get_latest_table_name(self, name, bucket=None):
        #name is in the form bucket # table # version
        if not self._authenticated:
            self.authenticate()
        if name.find("#")>=0 and not bucket:
            elements = name.split("#")
            if len(elements) ==3:
                bucket = elements[0]
                name = elements[1]
        elif not bucket:
            return name
        
        
        sql = (
                "select `bucket` as b, "
                "classname, "
                "max(`version`) as version "
                "from versions "
                "where classname = '{}' and `bucket`= '{}' "
            ).format(name, bucket)
        
        payload = {
            "command": sql,
            "language": "sql"
        }

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            if 'result' in result and len(result['result']) > 0:
                latest = result['result'][0]
                return f"{latest['b']}#{latest['classname']}#{latest['version']}"
            else:
                return f"{bucket}#{name}#0"
        except Exception as e:
            error_msg = f"Failed to get latest table name: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)

    def create_schema(self, schema_name, super_class="V"):
        if not self._authenticated:
            self.authenticate()

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name
        payload = {
            "command": f"CREATE DOCUMENT TYPE {schema_name} IF NOT EXISTS",
            "language": "sql"
        }

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            self.logger.debug("Schema %s created successfully", schema_name)
            return result
            
        except Exception as e:
            error_msg = f"Failed to create schema {schema_name}: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)

    def create_property(self, schema_name: str, field_name: str, field_type: str = "STRING"):
        """
        Create a new property (field) in the specified schema.
        
        Args:
            schema_name: Name of the schema
            field_name: Name of the field to create
            field_type: Type of the field (default is "STRING")
            
        Returns:
            Result of the property creation operation
            
        Raises:
            ArcadeDBError: If property creation fails
        """
        if not self._authenticated:
            self.authenticate()

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name
        payload = {
            "command": f"CREATE PROPERTY {schema_name}.`{field_name}` {field_type.upper()}",
            "language": "sql"
        }

        print(payload["command"])

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            self.logger.debug("Property %s created successfully in schema %s", field_name, schema_name)
            return result
            
        except Exception as e:
            error_msg = f"Failed to create property {field_name} in schema {schema_name}: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)   
    
    def read_incremental_data(self, schema_name, last_rid=None, versioning=True):
        if not self._authenticated:
            self.authenticate()

        if versioning:
            schema_name = self.get_latest_table_name(schema_name)

        numRows = self.count_values_schema(schema_name)
        self.logger.debug("numRows =  {} ".format(numRows))

        if numRows == 0:
            self.logger.warning("No records found in schema %s", schema_name)
            return []
        
        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name
        query = f"SELECT * FROM {schema_name}"

        payload = {
            "command": query,
            "language": "sql"
        }
        self.logger.debug("Executing query: %s", query)
        NEW_PAGE_SIZE = 20000
        limit = NEW_PAGE_SIZE if NEW_PAGE_SIZE > 0 else numRows

        paged_query = query
        if last_rid:
            paged_query += f" WHERE @rid > '{last_rid}'"

        paged_query += f" ORDER BY @rid LIMIT {limit}"

        payload['command'] = paged_query

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            data = response.json().get("result", [])
            if not data:
                return pd.DataFrame(), last_rid
            last_rid = data[-1]["@rid"]

            #if len(data) < limit:
            #    return pd.DataFrame(), last_rid
            # convert to dataframe
            results = pd.DataFrame.from_records(data)

            if '@type' in results.columns:
                results = results.drop(columns=['@type'])
            if '@cat' in results.columns:
                results = results.drop(columns=['@cat'])
            if '@rid' in results.columns:
                results = results.drop(columns=['@rid'])
            print("Number of results downloaded so far....: ", results.shape[0])
            return results, last_rid
        except Exception as e:
            error_msg = f"Failed to read data from schema {schema_name}: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)


    def read_data(
        self,
        schema_name,
        fields=None,
        customer_type_id=None,
        is_not_null=None,
        versioning=True,
        condition=None
    ):
        """
        read_data read a schema from ArcadeDB. The array of fields to read is received in
        the fields parameter. rid is always added as a parameter to be read.

        :param schema_name: Name of the schema
        :type schema_name: string
        :param fields: Array of json objects with the names of the fields to read
        :type fields: Array of JSON objects
        """

        if not self._authenticated:
            self.authenticate()

        if versioning:
            schema_name = self.get_latest_table_name(schema_name)

        #schema_name = f"{schema_name}"

        self.logger.debug("reading table %s", schema_name)
        numRows = self.count_values_schema(schema_name, customer_type_id, is_not_null)
        self.logger.debug("numRows =  {} ".format(numRows))

        if numRows == 0:
            self.logger.warning("No records found in schema %s", schema_name)
            return []

        if fields is None:
            fields = ["*"]
        else:
            fields = [f"`{field}`" for field in fields]

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name
        query = f"SELECT {', '.join(fields)} FROM {schema_name}"

        if condition is not None:
            query += f" WHERE {condition}"
        if customer_type_id is not None:
            if condition is None:
                query += f" WHERE CustomerTypeId = {customer_type_id}"
            else:
                query += f" AND CustomerTypeId = {customer_type_id}"

        if is_not_null is not None:
            query += f" AND {is_not_null} IS NOT NULL"

        payload = {
            "command": query,
            "language": "sql"
        }
        self.logger.debug("Executing query: %s", query)
        NEW_PAGE_SIZE = 20000
        limit = NEW_PAGE_SIZE if NEW_PAGE_SIZE > 0 else numRows

        results = []
        last_rid = None
        while True:
            paged_query = query
            if last_rid:
                paged_query += f" WHERE @rid > '{last_rid}'"

            paged_query += f" ORDER BY @rid LIMIT {limit}"

            payload['command'] = paged_query

            self.logger.debug("Query with pagination: %s", payload['command'])
            print("Number of results downloaded so far....: ", len(results))

            try:
                response = self._make_request('POST', f'command/{self.config.database}', payload)
                data = response.json().get("result", [])
                if not data:
                    break

                results.extend(data)
                last_rid = data[-1]["@rid"]

                if len(data) < limit:
                    break

            except Exception as e:
                error_msg = f"Failed to read data from schema {schema_name}: {str(e)}"
                self.logger.error(error_msg)
                raise ArcadeDBError(error_msg)
            
        if not results:
            self.logger.warning("No records retrieved from schema %s", schema_name)
            return pd.DataFrame()
        
        results = pd.DataFrame.from_records(results)
            # drop the @rid, @type @cat  fields if it exists
        #if '@rid' in result.columns:
        #    result = result.drop(columns=['@rid'])
        if '@type' in results.columns:
            results = results.drop(columns=['@type'])
        if '@cat' in results.columns:
            results = results.drop(columns=['@cat'])
        return results
    def get_latest_schema_name(self, schema_name: str) -> str:
        table_name = schema_name
        if schema_name.find("#")>=0:
            bucket = schema_name.split("#")[0]
            name = schema_name.split("#")[1]
            lastVersion, _ = self.get_next_version(name, bucket=bucket)
            table_name = f"{bucket}#{name}#{lastVersion}"
        return table_name

    def insert_dataframe(self, schema_name: str, data: pd.DataFrame, columns=None):
        if not self._authenticated:
            self.authenticate()

        if not columns:
            columns = get_column_names_from_df(data)

        if isinstance(columns, list):
            # check the first element of the list has a keys (name, type, index)
            if not all(key in columns[0] for key in ['name', 'type', 'index']):
                raise ArcadeDBError("Invalid columns format. Each column must have 'name', 'type', and 'index' keys.")
        elif columns is None:
            raise ArcadeDBError("Columns parameter must be provided as a JSON file or a list of dictionaries.")
        
        else:
            raise ArcadeDBError("Invalid columns format. Must be a JSON file or a list of dictionaries.")
        
        # Create properties for each column
        for column in columns:
            self.create_property(schema_name, column.get('name', 'Name'), column.get('type', 'STRING'))

        if data.empty:
            self.logger.warning("DataFrame is empty. No records to insert.")
            return
        self.logger.debug("Updating versions for table %s", schema_name)
        self.save_version(schema_name)
        self.logger.info("Inserting %d records into schema %s", len(data), schema_name)
        self.insert_data(schema_name, data, columns)
        print("Inserted records into schema %s", schema_name)
        return schema_name
        #self.index_data(table_name, columns)
        

    def insert_data(self, schema_name: str, data: pd.DataFrame, columns:list):
        """
        Insert data into the specified schema.
        
        Args:
            schema_name: Name of the schema to insert data into
            data: DataFrame containing data to insert
            
        Raises:
            ArcadeDBError: If insertion fails
        """
        if not self._authenticated:
            self.authenticate()

        if data.empty:
            self.logger.warning("DataFrame is empty. No records to insert.")
            return

        self.begin_transaction()
        self.logger.debug("Transaction started")

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name

        # total number of records to insert
        total_records = data.shape[0]

        columns_arr = data.columns.tolist()
        batch_size = BATCH_SIZE
        if len(columns_arr) < 10:
            batch_size = 1000
        # remove the first row
        data = data.iloc[1:]
        self.logger.info("Inserting %d records into schema %s", total_records, schema_name)

        formatted_columns = format_columns(columns)

        for i in range(0, total_records, batch_size):
            batch = data.iloc[i:i + batch_size]
            values = []
            for _, row in batch.iterrows():
                row_values = []
                for col in formatted_columns:
                    val = row[col]
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        row_values.append("NULL")
                    elif isinstance(val, list):
                        escaped_items = []
                        for v in val:
                            s = str(v).replace('"', '\\"')
                            escaped_items.append(f'"{s}"')
                        array_literal = "[" + ",".join(escaped_items) + "]"
                        row_values.append(array_literal)
                    elif isinstance(val, str):
                        # escape double quotes and single quotes
                        safe_val = val.replace('"', '""').replace("'", "''")
                        row_values.append(f"'{safe_val}'")
                    # --- Handle booleans ---
                    elif isinstance(val, bool):
                        row_values.append("true" if val else "false")
                    else:
                        row_values.append(str(val))
                values.append(f"({', '.join(row_values)})")

            SQL_STATEMENT = f"""
                INSERT INTO {schema_name} ({", ".join(formatted_columns)})
                VALUES {", ".join(values)};
            """

            payload = {
                "type": "cmd",
                "language": "sql",
                "command": SQL_STATEMENT,
                "serializer": "record"
            }
            
            try:
                response = self._make_request('POST', f'command/{self.config.database}', payload)
                if response.status_code == 200:
                    result = response.json()
                    if 'result' in result:
                        self.logger.debug("Inserted %d records into schema %s successfully", len(batch), schema_name)
                    else:
                        self.logger.warning("No result returned for batch insert into schema %s", schema_name)
                else:
                    self.logger.error("Failed to insert records into schema %s: %s", schema_name, response.text)
            except Exception as e:
                error_msg = f"Failed to insert records into schema {schema_name}: {str(e)}"
                self.logger.error(error_msg)
                self.rollback_transaction()
                raise ArcadeDBError(error_msg)

    def index_data(self, schema_name: str, columns: list):
        """
        Create indexes on specified columns in the schema.
        
        Args:
            schema_name: Name of the schema
            columns: List of column definitions
            
        Raises:
            ArcadeDBError: If index creation fails
        """
        if not self._authenticated:
            self.authenticate()

        for column in columns:
            if column.get('index', False):
                field_name = column.get('name')
                index_type = 'UNIQUE'
                payload = {
                    "command": f"CREATE INDEX ON `{schema_name}` ({field_name}) {index_type}",
                    "language": "sql"
                }
                try:
                    response = self._make_request('POST', f'command/{self.config.database}', payload)
                    result = response.json()
                    self.logger.debug("Index %s created successfully on schema %s", field_name, schema_name)
                except Exception as e:
                    error_msg = f"Failed to create index on {field_name} in schema {schema_name}: {str(e)}"
                    self.logger.error(error_msg)
                    raise ArcadeDBError(error_msg)
                
    def begin_transaction(self):
        """
        Begin a transaction in ArcadeDB.
        
        Returns:
            Transaction ID
            
        Raises:
            ArcadeDBError: If transaction initiation fails
        """
        if not self._authenticated:
           self.authenticate()

        try:
            response = self._make_request('POST', f'/begin/{self.config.database}')
            session_id = response.headers.get("arcadedb-session-id")

            if response.status_code != 204 or not session_id:
                error_msg = "Failed to begin transaction: No session ID returned"
                self.logger.error(error_msg)
                raise ArcadeDBError(error_msg)
            self.session.headers.update({
                "arcadedb-session-id": session_id
            })
            self.logger.debug("Transaction started successfully")

        except Exception as e:
            error_msg = f"Failed to begin transaction: {str(e)}"
            print(error_msg)
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def commit_transaction(self):
        """
        Commit the current transaction in ArcadeDB.
        
        Raises:
            ArcadeDBError: If transaction commit fails
        """
        if not self._authenticated: 
            self.authenticate()

        try:
            response = self._make_request('POST', f'/commit/{self.config.database}')
            self.logger.debug("Transaction committed successfully")
            return response.text
        except Exception as e:
            error_msg = f"Failed to commit transaction: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def rollback_transaction(self):
        """
        Rollback the current transaction in ArcadeDB.
        
        Raises:
            ArcadeDBError: If transaction rollback fails
        """
        if not self._authenticated:
            self.authenticate()

        try:
            response = self._make_request('POST', f'/rollback/{self.config.database}')
            self.logger.debug("Transaction rolled back successfully")
            return response.text
            
        except Exception as e:
            error_msg = f"Failed to rollback transaction: {str(e)}"
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

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name

        if customer_type_id == None:
            if is_not_null == None:
                limitQuery = "SELECT COUNT(*) AS counting from {}".format(schema_name)
            else:
                limitQuery = (
                    "SELECT COUNT(*) AS counting from {} WHERE {} IS NOT NULL".format(
                        schema_name, is_not_null
                    )
                )
        else:
            if is_not_null == None:
                limitQuery = "SELECT COUNT(*) AS counting from {} where CustomerTypeId = {} ".format(
                    schema_name, customer_type_id
                )
            else:
                limitQuery = "SELECT COUNT(*) AS counting from {} where CustomerTypeId = {} and {} IS NOT NULL ".format(
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
    
    def drop_schema(self, schema_name):
        """
        Drop a schema (class) from the database.
        
        Args:
            schema_name: Name of the schema to drop
            
        Returns:
            Result of the schema drop operation
            
        Raises:
            ArcadeDBError: If schema drop fails
        """
        if not self._authenticated:
            self.authenticate()

        schema_name = f"`{schema_name}`" if "#" in schema_name else schema_name

        payload = {
            "command": f"DROP TYPE {schema_name} IF EXISTS UNSAFE",
            "language": "sql"
            }
        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            self.logger.debug("Schema %s dropped successfully", schema_name)
            return result
            
        except Exception as e:
            error_msg = f"Failed to drop schema {schema_name}: {str(e)}"
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

        schema_name = f"`{schema}`" if "#" in schema else schema

        payload = {
            "command": f"UPDATE {schema_name} SET {field_name} = {value}",
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
        
    def save_version(self, table_name: str):
        """
        Saves the table name bucket and timestamp of the table in the versions table
        :params classname: name of the table to be stored in versions table.
        :params timestamp: (string) a timestamp string with the date of last modification of the file.
        :params version: number of version of the table
        :params bucket: name of the bucket and MDA where the tables comes from


        """
        if not self._authenticated:
            self.authenticate()
        parsed = self._parse_table_name(table_name)
        if not parsed:
            self.logger.error("Failed to parse table name: %s", table_name)
            return
        classname, timestamp, version, bucket = parsed

        sql = 'INSERT INTO versions set  classname = "{}", timestamp = "{}", version = {}, `bucket` = "{}" '.format(
                classname, timestamp, version, bucket
            )

        payload = {
            "command": sql,
            "language": "sql"
        }

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            self.logger.debug("Updated successfully in schema %s", classname)
            return result

        except Exception as e:
            error_msg = f"Failed to update counter: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)
        
    def close(self) -> None:
        """Close the client session."""
        if self.session:
            self.session.close()

    def _parse_table_name(self, table_name: str):
        parts = table_name.split("#")
        if len(parts) != 3:
            return None
        current_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        return parts[1], current_timestamp, parts[2], parts[0]

    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

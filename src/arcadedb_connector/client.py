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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import ArcadeDBConfig
from .utils import read_file_content, format_columns
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
        print(payload)
        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            result = response.json()
            print(result)
            print(result['result'])
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
        if name.find("#")>=0:
            elements = name.split("#")
            if len(elements) ==3:
                bucket = elements[0]
                name = elements[1]
        else:
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
        """
        Create a new schema (class) in the database.
        
        Args:
            schema_name: Name of the schema to create
            super_class: Super class for the new schema (default is "V")
            
        Returns:
            Result of the schema creation operation
            
        Raises:
            ArcadeDBError: If schema creation fails
        """
        if not self._authenticated:
            self.authenticate()

        payload = {
            "command": f"CREATE DOCUMENT TYPE `{schema_name}` IF NOT EXISTS",
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

        payload = {
            "command": f"CREATE PROPERTY `{schema_name}`.`{field_name}` {field_type.upper()}",
            "language": "sql"
        }

        print(payload)

        try:
            response = self._make_request('POST', f'command/{self.config.database}', payload)
            print("Response received: ")
            result = response.json()
            print(result)
            self.logger.debug("Property %s created successfully in schema %s", field_name, schema_name)
            return result
            
        except Exception as e:
            error_msg = f"Failed to create property {field_name} in schema {schema_name}: {str(e)}"
            self.logger.error(error_msg)
            raise ArcadeDBError(error_msg)   
   
    def read_data(
        self,
        schema_name,
        fields=None,
        customer_type_id=None,
        is_not_null=None,
        versioning=True
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

        query = f"SELECT {', '.join(fields)} FROM {schema_name}"

        if customer_type_id is not None:
            query += f" WHERE CustomerTypeId = {customer_type_id}"

        if is_not_null is not None:
            query += f" AND {is_not_null} IS NOT NULL"

        payload = {
            "command": query,
            "language": "sql"
        }
        self.logger.debug("Executing query: %s", query)

        skip = 0
        limit = PAGE_SIZE if PAGE_SIZE > 0 else numRows

        result = []
        while skip < numRows:
            payload['command'] = query
            if skip > 0:
                payload['command'] += f" SKIP {skip}"
            if limit > 0:
                payload['command'] += f" LIMIT {limit}"

            self.logger.debug("Query with pagination: %s", payload['command'])

            try:
                response = self._make_request('POST', f'command/{self.config.database}', payload)
                data = response.json()
                if 'result' in data:
                    result.extend(data['result'])
                    if skip == 0:
                        print("First page of results: ", data['result'])
                        # create a dataframe for the first records
                        result = pd.DataFrame(data['result'])
                        print("DataFrame created for first page of results:")
                    else:
                        print("Subsequent page of results: ", data['result'])
                        # append to the existing dataframe
                        result = pd.concat([result, pd.DataFrame(data['result'])], ignore_index=True)

                else:
                    self.logger.warning("No results found for query: %s", payload['command'])
                    break
                
                skip += limit
                if len(data['result']) < limit:
                    break

            except Exception as e:
                error_msg = f"Failed to read data from schema {schema_name}: {str(e)}"
                self.logger.error(error_msg)
                raise ArcadeDBError(error_msg)
            # drop the @rid, @type @cat  fields if it exists
        if '@rid' in result.columns:
            result = result.drop(columns=['@rid'])
        if '@type' in result.columns:
            result = result.drop(columns=['@type'])
        if '@cat' in result.columns:
            result = result.drop(columns=['@cat'])
        return result

    def insert_dataframe(self, schema_name: str, data: pd.DataFrame, columns=None):
        if not self._authenticated:
            self.authenticate()

        table_name = schema_name
        if schema_name.find("#")>=0:
            bucket = schema_name.split("#")[0]
            name = schema_name.split("#")[1]
            lastVersion, _ = self.get_next_version(name, bucket=bucket)
            table_name = f"{bucket}#{name}#{lastVersion}"

        print("Table name to be used: ", table_name)

        if table_name is None:
            raise ArcadeDBError(f"Table {schema_name} does not exist in the database.")
        
        if table_name.find("#") <= 0:
            self.drop_schema(table_name)
            print("Table {} dropped to be recreated.".format(table_name))

        self.create_schema(table_name)
        print("Schema {} created.".format(table_name))

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
            field_name = column.get('name')
            field_type = column.get('type', 'STRING')
            self.create_property(table_name, field_name, field_type)

        if data.empty:
            self.logger.warning("DataFrame is empty. No records to insert.")
            return
        self.logger.debug("Updating versions for table %s", table_name)
        print("Updating versions for table {}".format(table_name))
        self.save_version(table_name)
        self.logger.info("Inserting %d records into schema %s", len(data), table_name)
        self.insert_data(table_name, data, columns)
        

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

        print("Authenticated successfully.")

        if data.empty:
            self.logger.warning("DataFrame is empty. No records to insert.")
            return

        #self.begin_transaction()
        print("Transaction started.")
        self.logger.debug("Transaction started")

        # total number of records to insert
        total_records = data.shape[0]
        # remove the first row
        data = data.iloc[1:]
        self.logger.info("Inserting %d records into schema %s", total_records, schema_name)

        formatted_columns = format_columns(columns)
        print(formatted_columns)

        for i in range(0, total_records, BATCH_SIZE):
            batch = data.iloc[i:i + BATCH_SIZE]
            values = []
            for _, row in batch.iterrows():
                row_values = []
                for col in formatted_columns:
                    val = row[col]
                    if pd.isna(val):
                        row_values.append("NULL")
                    elif isinstance(val, str):
                        # escape double quotes and single quotes
                        safe_val = val.replace('"', '""').replace("'", "''")
                        row_values.append(f"'{safe_val}'")
                    else:
                        row_values.append(str(val))
                values.append(f"({', '.join(row_values)})")

            SQL_STATEMENT = f"""
                INSERT INTO `{schema_name}` ({", ".join(formatted_columns)})
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
                        print("Inserted Successfully")
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

    def begin_transaction(self):
        """
        Begin a transaction in ArcadeDB.
        
        Returns:
            Transaction ID
            
        Raises:
            ArcadeDBError: If transaction initiation fails
        """
        #if not self._authenticated:
        #    self.authenticate()

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
            print("Transaction committed successfully")
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

        payload = {
            "command": f"DROP TYPE `{schema_name}` IF EXISTS",
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

        print("Table name to be used: ", table_name)
        print(payload)

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
            self.logger.info("ArcadeDB client session closed")

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

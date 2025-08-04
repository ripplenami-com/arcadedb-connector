"""
Example usage of the ArcadeDB Connector.
"""

import logging
from arcadedb_connector import ArcadeDBClient, ArcadeDBConfig
from arcadedb_connector.exceptions import ArcadeDBError

# Set up logging to see what's happening
logging.basicConfig(level=logging.INFO)


def main():
    """Demonstrate basic usage of ArcadeDB Connector."""
    
    try:
        # Option 1: Create config from environment variables
        # Make sure you have a .env file with your settings
        # config = ArcadeDBConfig.from_env()
        
        # Option 2: Create config manually
        config = ArcadeDBConfig(
             host="localhost",
             port=2480,
             database="db",
             username="root",
             password="password",
             use_ssl=False,
             timeout=30
        )
        
        print(f"Configuration loaded: {config.to_dict()}")
        
        # Use client as context manager for automatic cleanup
        with ArcadeDBClient(config) as client:
            
            # Authenticate
            print("Authenticating...")
            #client.authenticate()
            return
            # Get server info
            print("Getting server info...")
            server_info = client.get_server_info()
            print(f"Server version: {server_info.get('version', 'Unknown')}")
            
            # List databases
            print("Listing databases...")
            databases = client.list_databases()
            print(f"Available databases: {databases}")
            
            # Example: Create a document
            print("Creating a document...")
            document_data = {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "age": 30,
                "created_at": "2025-01-01T00:00:00Z"
            }
            
            result = client.create_document("Person", document_data)
            print(f"Document created: {result}")
            
            # Example: Execute a query
            print("Executing a query...")
            query_result = client.execute_query(
                "SELECT * FROM Person WHERE age > :min_age",
                parameters={"min_age": 25}
            )
            print(f"Query result: {query_result}")
            
            # Example: Get document by RID (if you have one)
            # document = client.get_document("#1:0")
            # print(f"Retrieved document: {document}")
            
    except ArcadeDBError as e:
        print(f"ArcadeDB Error: {e.message}")
        if e.status_code:
            print(f"Status Code: {e.status_code}")
        if e.details:
            print(f"Details: {e.details}")
            
    except Exception as e:
        print(f"Unexpected error: {str(e)}")


if __name__ == "__main__":
    main()

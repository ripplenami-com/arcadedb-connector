# ArcadeDB Connector

A professional Python connector for ArcadeDB with configuration management, robust error handling, and comprehensive logging.

## Features

- 🔌 **Easy Connection Management**: Simple configuration and connection handling
- 🔐 **Secure Authentication**: Built-in authentication with credential management
- 🛡️ **Robust Error Handling**: Comprehensive exception hierarchy with detailed error information
- 📊 **Query Execution**: Support for SQL queries with parameter binding
- 📄 **Document Operations**: Create, read, and manage documents
- ⚙️ **Environment Configuration**: Flexible configuration via environment variables or code
- 🔄 **Retry Logic**: Automatic retry for transient failures
- 📝 **Comprehensive Logging**: Detailed logging for debugging and monitoring
- 🧪 **Well Tested**: Comprehensive test suite with high coverage

## Installation

```bash
pip install arcadedb-connector
```

For development:

```bash
pip install "arcadedb-connector[dev]"
```

## Quick Start

### 1. Configuration

Create a `.env` file (copy from `.env.example`):

```bash
# Required settings
ARCADEDB_DATABASE=your_database_name
ARCADEDB_USERNAME=your_username
ARCADEDB_PASSWORD=your_password

# Optional settings
ARCADEDB_HOST=localhost
ARCADEDB_PORT=2480
ARCADEDB_USE_SSL=false
ARCADEDB_TIMEOUT=30
ARCADEDB_MAX_RETRIES=3
```

### 2. Basic Usage

```python
from arcadedb_connector import ArcadeDBClient, ArcadeDBConfig

# Option 1: Load config from environment
config = ArcadeDBConfig.from_env()

# Option 2: Create config manually
config = ArcadeDBConfig(
    host="localhost",
    port=2480,
    database="mydb",
    username="admin",
    password="secret"
)

# Use the client
with ArcadeDBClient(config) as client:
    # Test connection
    client.connect()

    # Authenticate
    client.authenticate()

    # Execute queries
    result = client.execute_query("SELECT * FROM Person WHERE age > :min_age",
                                 parameters={"min_age": 25})

    # Create documents
    person = client.create_document("Person", {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    })

    # Get documents
    document = client.get_document("#1:0")
```

## Configuration

### Environment Variables

| Variable               | Required | Default     | Description                 |
| ---------------------- | -------- | ----------- | --------------------------- |
| `ARCADEDB_DATABASE`    | ✅       | -           | Database name               |
| `ARCADEDB_USERNAME`    | ✅       | -           | Username for authentication |
| `ARCADEDB_PASSWORD`    | ✅       | -           | Password for authentication |
| `ARCADEDB_HOST`        | ❌       | `localhost` | ArcadeDB server host        |
| `ARCADEDB_PORT`        | ❌       | `2480`      | ArcadeDB server port        |
| `ARCADEDB_USE_SSL`     | ❌       | `false`     | Use HTTPS/SSL               |
| `ARCADEDB_TIMEOUT`     | ❌       | `30`        | Request timeout in seconds  |
| `ARCADEDB_MAX_RETRIES` | ❌       | `3`         | Maximum retry attempts      |

### Programmatic Configuration

```python
from arcadedb_connector import ArcadeDBConfig

config = ArcadeDBConfig(
    host="arcadedb.example.com",
    port=2480,
    database="production_db",
    username="api_user",
    password="secure_password",
    use_ssl=True,
    timeout=60,
    max_retries=5
)
```

## API Reference

### ArcadeDBClient

#### Connection Methods

- `connect()` - Test connection to server
- `authenticate()` - Authenticate with credentials
- `close()` - Close client session

#### Query Methods

- `execute_query(query, parameters=None)` - Execute SQL query
- `create_document(bucket_name, document)` - Create new document
- `get_document(rid)` - Retrieve document by RID

#### Information Methods

- `get_server_info()` - Get server information
- `list_databases()` - List available databases

### Error Handling

The connector provides a comprehensive exception hierarchy:

```python
from arcadedb_connector.exceptions import (
    ArcadeDBError,                    # Base exception
    ArcadeDBConnectionError,          # Connection issues
    ArcadeDBAuthenticationError,      # Authentication failures
    ArcadeDBQueryError,              # Query execution errors
    ArcadeDBTimeoutError,            # Request timeouts
    ArcadeDBConfigurationError       # Configuration issues
)

try:
    client.execute_query("SELECT * FROM InvalidType")
except ArcadeDBQueryError as e:
    print(f"Query failed: {e.message}")
    print(f"Status code: {e.status_code}")
except ArcadeDBError as e:
    print(f"General error: {e.message}")
```

## Advanced Usage

### Custom Logging

```python
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('arcadedb_connector')
logger.setLevel(logging.DEBUG)

# Now all client operations will be logged
```

### Connection Pooling and Retry Logic

The client automatically handles:

- Connection retries with exponential backoff
- Request timeouts
- Authentication token management
- HTTP connection pooling

### Query Parameter Binding

```python
# Safe parameter binding
result = client.execute_query(
    "SELECT * FROM Person WHERE name = :name AND age > :min_age",
    parameters={
        "name": "John Doe",
        "min_age": 25
    }
)

# Complex parameters
result = client.execute_query(
    "INSERT INTO Person SET name = :name, metadata = :metadata",
    parameters={
        "name": "Jane",
        "metadata": {"department": "Engineering", "level": "Senior"}
    }
)
```

## Development

### Setting Up Development Environment

```bash
# Clone the repository
git clone <repository-url>
cd arcadedb-connector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=arcadedb_connector

# Run specific test file
pytest tests/test_client.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

## Examples

See the `examples/` directory for more comprehensive examples:

- `basic_usage.py` - Basic connection and operations
- `advanced_queries.py` - Complex query examples
- `error_handling.py` - Error handling patterns
- `batch_operations.py` - Bulk operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### v0.1.0

- Initial release
- Basic connection and authentication
- Query execution with parameter binding
- Document CRUD operations
- Comprehensive error handling
- Environment-based configuration
- Full test suite

## Support

For issues and questions:

- Open an issue on GitHub
- Check the documentation
- Review the examples

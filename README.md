# ArcadeDB Connector

A professional Python connector for ArcadeDB with configuration management, robust error handling, and comprehensive logging. This package provides a simple yet powerful interface to interact with ArcadeDB databases, supporting both basic and advanced operations.

## 🏗️ Project Structure

```
arcadedb-connector/
├── LICENSE                     # MIT License
├── README.md                   # This documentation
├── pyproject.toml             # Project configuration and dependencies
├── poetry.lock                # Locked dependency versions
├── src/                       # Source code directory
│   └── arcadedb_connector/    # Main package
│       ├── __init__.py        # Package initialization and exports
│       ├── client.py          # Main ArcadeDB client implementation
│       ├── config.py          # Configuration management with Pydantic
│       ├── constants.py       # Package constants (PAGE_SIZE, BATCH_SIZE)
│       ├── exceptions.py      # Custom exception hierarchy
│       └── utils.py           # Utility functions for data processing
├── tests/                     # Test suite
│   ├── __init__.py           # Test package initialization
│   ├── test_client.py        # Client functionality tests
│   ├── test_config.py        # Configuration tests
│   └── test_utils.py         # Utility functions tests
└── examples/                  # Usage examples
    └── basic_usage.py        # Basic connector usage example
```

## 📦 Core Components

### 🔧 **src/arcadedb_connector/**

#### `client.py` - Main Client Implementation

- **ArcadeDBClient**: Primary interface for ArcadeDB operations
- Connection management with automatic retry logic
- HTTP session handling with connection pooling
- Authentication and session management
- Query execution with parameter binding
- Document CRUD operations (Create, Read, Update, Delete)
- Bulk operations for data loading
- Comprehensive error handling and logging

#### `config.py` - Configuration Management

- **ArcadeDBConfig**: Pydantic-based configuration model
- Environment variable support with `.env` file loading
- Configuration validation with type checking
- Default values and field validation
- Support for both programmatic and environment-based configuration

#### `exceptions.py` - Exception Hierarchy

- **ArcadeDBError**: Base exception for all ArcadeDB operations
- **ArcadeDBConnectionError**: Network and connection issues
- **ArcadeDBAuthenticationError**: Authentication failures
- **ArcadeDBQueryError**: SQL query execution errors
- **ArcadeDBTimeoutError**: Request timeout handling
- **ArcadeDBConfigurationError**: Configuration validation errors

#### `utils.py` - Utility Functions

- **validate_rid()**: ArcadeDB Record ID validation
- **format_query_parameters()**: Query parameter formatting
- **build_where_clause()**: Dynamic WHERE clause generation
- **sanitize_identifier()**: Database identifier sanitization
- **parse_error_response()**: Error response parsing
- **read_file_content()**: File reading utilities
- **format_columns()**: Column formatting for insert operations

#### `constants.py` - Package Constants

- **PAGE_SIZE**: Default pagination size (1000)
- **BATCH_SIZE**: Default batch operation size (100)

### 🧪 **tests/** - Test Suite

#### `test_client.py` - Client Tests

- Connection and authentication testing
- Query execution tests with mocked responses
- Document operation tests
- Error handling and exception testing
- Retry logic validation
- Session management tests

#### `test_config.py` - Configuration Tests

- Environment variable loading
- Configuration validation
- Default value testing
- Error handling for invalid configurations

#### `test_utils.py` - Utility Tests

- RID validation testing
- Query parameter formatting
- WHERE clause building
- Identifier sanitization
- Error response parsing

### 📚 **examples/** - Usage Examples

#### `basic_usage.py` - Getting Started

- Configuration setup examples
- Basic connection and authentication
- Simple query execution
- Document operations
- Error handling patterns

## ✨ Features

- 🔌 **Easy Connection Management**: Simple configuration and connection handling
- 🔐 **Secure Authentication**: Built-in authentication with credential management
- 🛡️ **Robust Error Handling**: Comprehensive exception hierarchy with detailed error information
- 📊 **Query Execution**: Support for SQL queries with parameter binding
- 📄 **Document Operations**: Create, read, and manage documents
- ⚙️ **Environment Configuration**: Flexible configuration via environment variables or code
- 🔄 **Retry Logic**: Automatic retry for transient failures
- 📝 **Comprehensive Logging**: Detailed logging for debugging and monitoring
- 🧪 **Well Tested**: Comprehensive test suite with high coverage
- 📊 **Pandas Integration**: Built-in support for pandas DataFrames
- 🔄 **Bulk Operations**: Efficient batch processing for large datasets

## 🚀 Installation

```bash
pip install arcadedb-connector
```

For development with all testing and linting tools:

```bash
pip install "arcadedb-connector[dev]"
```

## 🔧 Development Setup

### Prerequisites

- Python 3.10 or higher
- Poetry (recommended) or pip for dependency management

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

# Or using Poetry
poetry install --with dev
```

### Project Dependencies

#### Core Dependencies

- **requests** (≥2.31.0): HTTP client for ArcadeDB REST API
- **python-dotenv** (≥1.0.0): Environment variable management
- **pydantic** (≥2.0.0): Data validation and configuration management
- **typing-extensions** (≥4.0.0): Extended type annotations
- **pandas** (≥2.3.1,<3.0.0): Data manipulation and analysis

#### Development Dependencies

- **pytest** (≥7.0.0): Testing framework
- **pytest-cov** (≥4.0.0): Test coverage reporting
- **black** (≥23.0.0): Code formatting
- **isort** (≥5.0.0): Import sorting
- **flake8** (≥6.0.0): Code linting
- **mypy** (≥1.0.0): Static type checking

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
| `ARCADEDB_HOST`        | ✅       | `localhost` | ArcadeDB server host        |
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

## 🛠️ Development Workflow

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=arcadedb_connector --cov-report=html

# Run specific test categories
pytest -m unit                    # Unit tests only
pytest -m integration            # Integration tests only
pytest -m "not integration"      # Exclude integration tests

# Run specific test file
pytest tests/test_client.py

# Run with verbose output
pytest -v --tb=short
```

### Code Quality Tools

```bash
# Format code with Black
black src/ tests/ examples/

# Sort imports with isort
isort src/ tests/ examples/

# Lint code with flake8
flake8 src/ tests/ examples/

# Type checking with mypy
mypy src/

# Run all quality checks
black src/ tests/ examples/ && \
isort src/ tests/ examples/ && \
flake8 src/ tests/ examples/ && \
mypy src/ && \
pytest --cov=arcadedb_connector
```

### Testing Strategy

The project follows a comprehensive testing approach:

- **Unit Tests**: Test individual functions and methods in isolation
- **Integration Tests**: Test component interactions (marked with `@pytest.mark.integration`)
- **Mock Testing**: HTTP requests are mocked to avoid dependency on external services
- **Coverage**: Aim for >90% test coverage
- **Fixtures**: Reusable test setup in `conftest.py` and individual test files

## 📖 Examples and Use Cases

The `examples/` directory contains comprehensive usage examples:

### Available Examples

- **`basic_usage.py`**:
  - Configuration setup and environment loading
  - Basic connection and authentication
  - Simple CRUD operations
  - Error handling patterns
  - Query execution with parameter binding

### Sample Usage Patterns

```python
# Example: Bulk data loading with error handling
import pandas as pd
from arcadedb_connector import ArcadeDBClient, ArcadeDBConfig
from arcadedb_connector.exceptions import ArcadeDBError

def load_csv_data(client: ArcadeDBClient, csv_path: str, bucket_name: str):
    """Load CSV data into ArcadeDB with error handling."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path)

        # Convert to records for bulk insert
        records = df.to_dict('records')

        # Bulk insert with error handling
        results = client.bulk_insert(bucket_name, records)
        print(f"Successfully inserted {len(results)} records")

    except ArcadeDBError as e:
        print(f"Database error: {e.message}")
    except Exception as e:
        print(f"Unexpected error: {e}")

# Example: Advanced querying with pagination
def paginated_query(client: ArcadeDBClient, query: str, page_size: int = 1000):
    """Execute paginated queries for large datasets."""
    offset = 0
    has_more = True

    while has_more:
        paginated_query = f"{query} SKIP {offset} LIMIT {page_size}"
        result = client.execute_query(paginated_query)

        if result.get('result'):
            yield result['result']
            offset += page_size
            has_more = len(result['result']) == page_size
        else:
            has_more = False
```

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Contribution Process

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following the project conventions
4. **Add tests** for new functionality
5. **Run the full test suite** and ensure all tests pass
6. **Update documentation** if necessary
7. **Submit a pull request** with a clear description

### Development Standards

- **Code Style**: Follow PEP 8, use Black for formatting
- **Type Hints**: Add type annotations for all functions
- **Documentation**: Include docstrings for all public methods
- **Testing**: Maintain >90% test coverage
- **Commits**: Use clear, descriptive commit messages

### Project Conventions

- **Naming**: Use snake_case for functions and variables
- **Imports**: Organize imports with isort
- **Error Handling**: Use specific exceptions from the hierarchy
- **Logging**: Use the configured logger, avoid print statements

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📚 API Documentation

### Package Structure

```python
from arcadedb_connector import (
    ArcadeDBClient,          # Main client class
    ArcadeDBConfig,          # Configuration management
    ArcadeDBError,           # Base exception
    ArcadeDBConnectionError, # Connection errors
    ArcadeDBAuthenticationError,  # Auth errors
    ArcadeDBQueryError,      # Query errors
    ArcadeDBTimeoutError     # Timeout errors
)
```

## 📋 Changelog

### v0.1.0 - Initial Release

#### Features

- ✅ Core ArcadeDB client implementation
- ✅ Pydantic-based configuration management
- ✅ Comprehensive exception hierarchy
- ✅ HTTP session management with retry logic
- ✅ Query execution with parameter binding
- ✅ Document CRUD operations
- ✅ Bulk data operations
- ✅ Environment-based configuration
- ✅ Comprehensive test suite (>90% coverage)
- ✅ Type annotations throughout
- ✅ Pandas integration for data processing
- ✅ Professional logging setup

#### Technical Highlights

- **Architecture**: Clean separation of concerns with dedicated modules
- **Error Handling**: Hierarchical exception system for specific error types
- **Testing**: Comprehensive test suite with unit and integration tests
- **Documentation**: Full API documentation with examples
- **Type Safety**: Complete type annotations for better IDE support

## 🆘 Support

### Getting Help

- **Documentation**: Check this README and code docstrings
- **Examples**: Review the `examples/` directory
- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Tests**: Run the test suite to understand expected behavior

### Common Issues

1. **Connection Errors**: Verify ArcadeDB server is running and accessible
2. **Authentication**: Check username/password and database permissions
3. **Configuration**: Ensure all required environment variables are set
4. **Dependencies**: Install with `pip install "arcadedb-connector[dev]"` for development

### Performance Tips

- Use bulk operations for large datasets
- Enable connection pooling (default in the client)
- Configure appropriate timeout values
- Use pagination for large query results
- Monitor logs for performance insights

---

**Built with ❤️ for the ArcadeDB community**

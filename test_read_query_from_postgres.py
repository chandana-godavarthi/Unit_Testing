
import pytest
from unittest.mock import patch, MagicMock
import common
from common import read_query_from_postgres

# Fixture to mock SparkSession
@pytest.fixture
def mock_spark():
    with patch.object(common, "spark") as mock_spark:
        yield mock_spark

# Fixture to inject required global variables
@pytest.fixture
def mock_globals():
    with patch.dict(common.__dict__, {
        "refDBjdbcURL": "jdbc:postgresql://localhost",
        "refDBname": "testdb",
        "refDBuser": "testuser",
        "refDBpwd": "testpwd"
    }):
        yield

# Test: Successful query execution
def test_read_query_success(mock_spark, mock_globals):
    mock_df = MagicMock()
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    result = read_query_from_postgres("SELECT * FROM users")

    mock_spark.read.format.assert_called_once_with("jdbc")
    mock_reader.option.assert_any_call("query", "SELECT * FROM users")
    mock_reader.load.assert_called_once()
    assert result == mock_df

# Test: Empty query string
def test_read_query_empty(mock_spark, mock_globals):
    mock_df = MagicMock()
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    result = read_query_from_postgres("")
    mock_reader.option.assert_any_call("query", "")
    assert result == mock_df

# Test: Exception during option chaining
def test_read_query_exception(mock_spark, mock_globals):
    mock_reader = MagicMock()
    mock_reader.option.side_effect = Exception("Query execution failed")
    mock_spark.read.format.return_value = mock_reader

    with pytest.raises(Exception, match="Query execution failed"):
        read_query_from_postgres("SELECT * FROM users")
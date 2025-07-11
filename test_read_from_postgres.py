
import pytest
from unittest.mock import patch, MagicMock
import common
from common import read_from_postgres

# Fixture to mock the SparkSession used in the function
@pytest.fixture
def mock_spark():
    with patch.object(common, "spark") as mock_spark:
        yield mock_spark

# Fixture to inject required global variables into the common module
@pytest.fixture
def mock_globals():
    with patch.dict(common.__dict__, {
        "refDBjdbcURL": "jdbc:postgresql://localhost",
        "refDBname": "testdb",
        "refDBuser": "testuser",
        "refDBpwd": "testpwd"
    }):
        yield

# Test case: Successful read from PostgreSQL table
def test_read_from_postgres_success(mock_spark, mock_globals):
    mock_df = MagicMock()
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader  # Chainable .option() calls
    mock_reader.load.return_value = mock_df        # .load() returns a DataFrame
    mock_spark.read.format.return_value = mock_reader

    result = read_from_postgres("my_table")

    mock_spark.read.format.assert_called_once_with("jdbc")
    assert mock_reader.option.call_count == 8
    mock_reader.option.assert_any_call("dbtable", "my_table")
    mock_reader.load.assert_called_once()
    assert result == mock_df

# Test case: Empty table name should still pass the empty string to dbtable
def test_read_from_postgres_empty_table_name(mock_spark, mock_globals):
    mock_df = MagicMock()
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df
    mock_spark.read.format.return_value = mock_reader

    result = read_from_postgres("")
    mock_reader.option.assert_any_call("dbtable", "")
    assert result == mock_df

# Test case: Simulate an exception during option chaining
def test_read_from_postgres_exception(mock_spark, mock_globals):
    mock_reader = MagicMock()
    mock_reader.option.side_effect = Exception("Connection failed")
    mock_spark.read.format.return_value = mock_reader

    with pytest.raises(Exception, match="Connection failed"):
        read_from_postgres("my_table")
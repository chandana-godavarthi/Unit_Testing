import pytest
from unittest.mock import patch, MagicMock
import common
from common import write_to_postgres

# Fixture to mock Spark DataFrame
@pytest.fixture
def mock_df():
    return MagicMock()

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

# Test: Successful write to PostgreSQL
def test_write_to_postgres_success(mock_df, mock_globals):
    mock_writer = MagicMock()
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_df.write.format.return_value = mock_writer

    write_to_postgres(mock_df, "users")

    mock_df.write.format.assert_called_once_with("jdbc")
    mock_writer.option.assert_any_call("dbtable", "users")
    mock_writer.mode.assert_called_once_with("append")
    mock_writer.save.assert_called_once()

# Test: Exception during write
def test_write_to_postgres_exception(mock_df, mock_globals):
    mock_writer = MagicMock()
    mock_writer.option.side_effect = Exception("Write failed")
    mock_df.write.format.return_value = mock_writer

    with pytest.raises(Exception, match="Write failed"):
        write_to_postgres(mock_df, "users")
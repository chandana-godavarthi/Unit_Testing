import pytest
from unittest.mock import patch, MagicMock
import common
from common import update_to_postgres

# Fixture to inject required global variables
@pytest.fixture
def mock_globals():
    with patch.dict(common.__dict__, {
        "refDBname": "testdb",
        "refDBuser": "testuser",
        "refDBpwd": "testpwd"
    }):
        yield

# Test: Successful query execution
def test_update_to_postgres_success(mock_globals):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("common.psycopg2.connect", return_value=mock_conn) as mock_connect:
        update_to_postgres("UPDATE users SET active = true")

        mock_connect.assert_called_once_with(
            dbname="testdb",
            user="testuser",
            password="testpwd",
            host="psql-pg-flex-tpconsole-dev-1.postgres.database.azure.com",
            sslmode="require"
        )
        mock_cursor.execute.assert_called_once_with("UPDATE users SET active = true")
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

# Test: Exception during query execution
def test_update_to_postgres_exception(mock_globals):
    with patch("common.psycopg2.connect", side_effect=Exception("Connection failed")):
        with pytest.raises(Exception, match="Connection failed"):
            update_to_postgres("UPDATE users SET active = true")
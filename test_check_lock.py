import pytest
import builtins
import time as time_module
from unittest.mock import MagicMock, patch
from common import check_lock

# Test when no lock is present
def test_check_lock_no_lock():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 0
    mock_spark.sql.return_value = mock_df

    with patch.dict(check_lock.__globals__, {
        'spark': mock_spark,
        'catalog_name': 'cdl_tp_dev',
        'time': time_module
    }):
        result = check_lock(RUN_ID=101, check_path="'/path/to/resource'")
        assert result == "'/path/to/resource'"
        mock_spark.sql.assert_called()

# Test when lock is present and retry succeeds
def test_check_lock_with_retry():
    mock_spark = MagicMock()

    mock_df_locked = MagicMock()
    mock_df_locked.count.return_value = 1

    mock_df_unlocked = MagicMock()
    mock_df_unlocked.count.return_value = 0

    mock_spark.sql.side_effect = [mock_df_locked, mock_df_unlocked, mock_df_unlocked]

    with patch.dict(check_lock.__globals__, {
        'spark': mock_spark,
        'catalog_name': 'cdl_tp_dev',
        'time': time_module
    }), patch.object(time_module, 'sleep', return_value=None):
        result = check_lock(RUN_ID=102, check_path="'/path/to/retry'")
        assert result == "'/path/to/retry'"
        assert mock_spark.sql.call_count >= 2

# Test exception handling during Spark SQL
def test_check_lock_exception():
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("Spark failure")

    with patch.dict(check_lock.__globals__, {
        'spark': mock_spark,
        'catalog_name': 'cdl_tp_dev',
        'time': time_module
    }), patch.object(time_module, 'sleep', return_value=None):
        with pytest.raises(Exception, match="Spark failure"):
            check_lock(RUN_ID=103, check_path="'/path/to/error'")
import pytest
from unittest.mock import MagicMock
import common  

def setup_module(module):
    """Set up global spark mock."""
    setattr(common, 'spark', MagicMock())

def test_release_semaphore_query_format_single_path():
    catalog_name = 'test_catalog'
    run_id = 101
    lock_path = "'/tmp/lock1'"  # Must be quoted for SQL IN clause

    expected_query = (
        "DELETE FROM test_catalog.internal_tp.tp_run_lock_plc "
        "WHERE run_id = 101 AND lock_path IN ('/tmp/lock1')"
    )

    common.release_semaphore(catalog_name, run_id, lock_path)
    actual_query = common.spark.sql.call_args[0][0].strip()
    assert actual_query == expected_query

def test_release_semaphore_query_format_multiple_paths():
    catalog_name = 'test_catalog'
    run_id = 202
    lock_path = "'/tmp/lock1','/tmp/lock2'"  # Comma-separated quoted paths

    expected_query = (
        "DELETE FROM test_catalog.internal_tp.tp_run_lock_plc "
        "WHERE run_id = 202 AND lock_path IN ('/tmp/lock1','/tmp/lock2')"
    )

    common.release_semaphore(catalog_name, run_id, lock_path)
    actual_query = common.spark.sql.call_args[0][0].strip()
    assert actual_query == expected_query

def test_release_semaphore_sql_error():
    catalog_name = 'test_catalog'
    run_id = 303
    lock_path = "'/tmp/lock1'"

    mock_func = MagicMock(side_effect=Exception("SQL execution failed"))
    setattr(common, 'spark', MagicMock(sql=mock_func))

    with pytest.raises(Exception, match="SQL execution failed"):
        common.release_semaphore(catalog_name, run_id, lock_path)
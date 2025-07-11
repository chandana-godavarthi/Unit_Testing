import pytest
from unittest.mock import MagicMock
from pyspark.sql import Row
from common import semaphore_queue

# Define a global RUN_ID so it can be referenced inside the mock_lit function
RUN_ID = 123

# Pytest fixture to set up all necessary mocks before each test
@pytest.fixture
def setup_mocks():
    # Mock for lit(RUN_ID).cast("bigint")
    mock_lit_run_id = MagicMock()
    mock_lit_run_id.cast.return_value = "casted_run_id"

    # Mock for lit(False)
    mock_lit_false = MagicMock()

    # Mock for current_timestamp()
    mock_current_timestamp = MagicMock(return_value="current_timestamp()")

    # Custom mock_lit function to return appropriate mock objects
    def mock_lit(x):
        if x == RUN_ID:
            return mock_lit_run_id
        elif x is False:
            return mock_lit_false
        else:
            return f"lit({x})"  # For other values, return a string for simplicity

    # Mock DataFrame and its chained methods
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = lambda col, val: mock_df  # Chainable
    mock_df.createOrReplaceTempView.return_value = None

    # Mock the write operation chain: write.format().mode().saveAsTable()
    mock_write = MagicMock()
    mock_format = MagicMock()
    mock_mode = MagicMock()

    mock_df.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.mode.return_value = mock_mode
    mock_mode.saveAsTable.return_value = None

    # Mock SparkSession and its createDataFrame method
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    # Inject mocks into the semaphore_queue function's module
    import common
    common.spark = mock_spark
    common.lit = mock_lit
    common.current_timestamp = mock_current_timestamp
    common.catalog_name = "cdl_tp_dev"

    # Return all mocks for use in assertions
    return {
        "mock_spark": mock_spark,
        "mock_df": mock_df,
        "mock_lit_run_id": mock_lit_run_id,
        "mock_lit_false": mock_lit_false,
        "mock_write": mock_write,
        "mock_format": mock_format,
        "mock_mode": mock_mode
    }

# Test case: multiple paths
def test_multiple_paths(setup_mocks):
    paths = ["path1", "path2", "path3"]
    expected_check_path = "'path1', 'path2', 'path3'"

    result = semaphore_queue(RUN_ID, paths)

    setup_mocks["mock_spark"].createDataFrame.assert_called_once_with(
        [Row(lock_path='path1'), Row(lock_path='path2'), Row(lock_path='path3')]
    )
    assert result == expected_check_path

# Test case: single path
def test_single_path(setup_mocks):
    paths = ["single_path"]
    expected_check_path = "'single_path'"

    result = semaphore_queue(RUN_ID, paths)

    setup_mocks["mock_spark"].createDataFrame.assert_called_once_with(
        [Row(lock_path='single_path')]
    )
    assert result == expected_check_path

# Test case: empty path list
def test_empty_paths(setup_mocks):
    paths = []
    expected_check_path = ""

    result = semaphore_queue(RUN_ID, paths)

    setup_mocks["mock_spark"].createDataFrame.assert_called_once_with([])
    assert result == expected_check_path

# Test case: verify all columns are added correctly
def test_column_additions(setup_mocks):
    paths = ["pathA", "pathB"]

    semaphore_queue(RUN_ID, paths)

    mock_df = setup_mocks["mock_df"]
    mock_df.withColumn.assert_any_call("run_id", "casted_run_id")
    mock_df.withColumn.assert_any_call("lock_sttus", setup_mocks["mock_lit_false"])
    mock_df.withColumn.assert_any_call("creat_date", "current_timestamp()")

# Test case: verify write operations to the table
def test_table_write_operations(setup_mocks):
    paths = ["pathX"]

    semaphore_queue(RUN_ID, paths)

    setup_mocks["mock_write"].format.assert_called_once_with("delta")
    setup_mocks["mock_format"].mode.assert_called_once_with("append")
    setup_mocks["mock_mode"].saveAsTable.assert_called_once_with("cdl_tp_dev.internal_tp.tp_run_lock_plc")
import pytest
from unittest.mock import patch
from common import semaphore_acquisition

# Mock implementations
def mock_semaphore_queue(run_id, paths):
    return [f"{path}_queued" for path in paths]

def mock_check_lock(run_id, paths):
    if paths is None:
        return None
    return [f"{path}_locked" for path in paths]

# Test case: Normal flow
@patch('common.semaphore_queue', side_effect=mock_semaphore_queue)
@patch('common.check_lock', side_effect=mock_check_lock)
def test_semaphore_acquisition_normal(mock_lock, mock_queue):
    run_id = "RUN001"
    paths = ["path1", "path2"]
    expected = ["path1_queued_locked", "path2_queued_locked"]
    result = semaphore_acquisition(run_id, paths)
    assert result == expected

# Test case: Empty paths
@patch('common.semaphore_queue', side_effect=mock_semaphore_queue)
@patch('common.check_lock', side_effect=mock_check_lock)
def test_semaphore_acquisition_empty_paths(mock_lock, mock_queue):
    run_id = "RUN002"
    paths = []
    expected = []
    result = semaphore_acquisition(run_id, paths)
    assert result == expected
# Test case: Single path
@patch('common.semaphore_queue', side_effect=mock_semaphore_queue)
@patch('common.check_lock', side_effect=mock_check_lock)
def test_semaphore_acquisition_single_path(mock_lock, mock_queue):
    run_id = "RUN003"
    paths = ["pathX"]
    expected = ["pathX_queued_locked"]
    result = semaphore_acquisition(run_id, paths)
    assert result == expected

# Test case: semaphore_queue returns None
@patch('common.semaphore_queue', side_effect=lambda run_id, paths: None)
@patch('common.check_lock', side_effect=mock_check_lock)
def test_semaphore_queue_returns_none(mock_lock, mock_queue):
    run_id = "RUN004"
    paths = ["pathY"]
    result = semaphore_acquisition(run_id, paths)
    assert result is None
# Test case: check_lock returns None
@patch('common.semaphore_queue', side_effect=mock_semaphore_queue)
@patch('common.check_lock', side_effect=lambda run_id, paths: None)
def test_check_lock_returns_none(mock_lock, mock_queue):
    run_id = "RUN005"
    paths = ["pathZ"]
    result = semaphore_acquisition(run_id, paths)
    assert result is None
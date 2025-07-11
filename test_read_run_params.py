
import pytest
from unittest.mock import patch
import argparse
from common import read_run_params

# Fixture for valid arguments
@pytest.fixture
def valid_args():
    return argparse.Namespace(
        FILE_NAME="data.csv",
        CNTRT_ID="contract_123",
        RUN_ID="run_001"
    )

# Fixture for missing arguments (None values)
@pytest.fixture
def missing_args():
    return argparse.Namespace(
        FILE_NAME=None,
        CNTRT_ID=None,
        RUN_ID=None
    )

def test_read_run_params_with_valid_args(valid_args):
    """
     Test Case: Valid CLI arguments
    Ensures that the function correctly parses and returns expected values.
    """
    with patch("argparse.ArgumentParser.parse_args", return_value=valid_args):
        args = read_run_params()
        assert args.FILE_NAME == "data.csv"
        assert args.CNTRT_ID == "contract_123"
        assert args.RUN_ID == "run_001"

def test_read_run_params_with_missing_args(missing_args):
    """
     Test Case: Missing CLI arguments
    Ensures that the function handles missing arguments gracefully (returns None).
    """
    with patch("argparse.ArgumentParser.parse_args", return_value=missing_args):
        args = read_run_params()
        assert args.FILE_NAME is None
        assert args.CNTRT_ID is None
        assert args.RUN_ID is None

def test_read_run_params_parse_failure():
    """
     Test Case: Argparse failure
    Simulates a SystemExit raised by argparse (e.g., due to invalid CLI usage).
    """
    with patch("argparse.ArgumentParser.parse_args", side_effect=SystemExit):
        with pytest.raises(SystemExit):
            read_run_params()
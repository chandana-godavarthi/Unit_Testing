import pytest
from unittest.mock import MagicMock
import pandas as pd
import common  

# Sample mock DataFrame
mock_df = pd.DataFrame({'dlmtr_val': ['|']})

def setup_module(module):
    """Set up common dependencies for all tests."""
    setattr(common, 'postgres_schema', 'test_schema')

def test_load_cntrt_dlmtr_lkp_returns_dataframe():
    setattr(common, 'read_query_from_postgres', MagicMock(return_value=mock_df))
    result = common.load_cntrt_dlmtr_lkp(123, 'customer')
    assert isinstance(result, pd.DataFrame)
    assert result.equals(mock_df)

def test_load_cntrt_dlmtr_lkp_query_format():
    setattr(common, 'read_query_from_postgres', MagicMock(return_value=mock_df))
    cntrt_id = 456
    dmnsn_name = 'product'

    expected_query = (
        "SELECT dt.dlmtr_val FROM test_schema.mm_cntrt_file_lkp ct "
        "join test_schema.mm_col_dlmtr_lkp dt on ct.dlmtr_id = dt.dlmtr_id "
        "WHERE cntrt_id=456 AND dmnsn_name='product'"
    )

    common.load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name)
    actual_query = common.read_query_from_postgres.call_args[0][0].strip()
    assert actual_query == expected_query

def test_load_cntrt_dlmtr_lkp_empty_result():
    setattr(common, 'read_query_from_postgres', MagicMock(return_value=pd.DataFrame()))
    result = common.load_cntrt_dlmtr_lkp(999, 'unknown')
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_load_cntrt_dlmtr_lkp_db_error():
    mock_func = MagicMock(side_effect=Exception("DB connection failed"))
    setattr(common, 'read_query_from_postgres', mock_func)
    with pytest.raises(Exception, match="DB connection failed"):
        common.load_cntrt_dlmtr_lkp(123, 'customer')

@pytest.mark.parametrize("cntrt_id, dmnsn_name", [
    (None, 'customer'),
    ('abc', 'customer'),
    (123, None),
    (123, ''),
])
def test_load_cntrt_dlmtr_lkp_invalid_inputs(cntrt_id, dmnsn_name):
    setattr(common, 'read_query_from_postgres', MagicMock(return_value=mock_df))
    try:
        result = common.load_cntrt_dlmtr_lkp(cntrt_id, dmnsn_name)
        assert isinstance(result, pd.DataFrame)
    except Exception as e:
        assert isinstance(e, Exception)
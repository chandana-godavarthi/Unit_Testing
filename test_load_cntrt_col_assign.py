import pytest
import pandas as pd
import common
from common import load_cntrt_col_assign

# More realistic mock DataFrame to simulate database return
mock_df = pd.DataFrame({
    'contract_id': [101],
    'column_name': ['customer_name'],
    'data_type': ['VARCHAR'],
    'is_required': [True],
    'default_value': ['N/A'],
    'validation_rule': ['NOT NULL']
})

def test_load_cntrt_col_assign_valid():
    """Test with a valid contract ID and expected query result."""
    cntrt_id = 101

    def mock_read_query(query):
        expected_query = f"select * from test_schema.mm_col_asign_lkp where cntrt_id = {cntrt_id}"
        assert query == expected_query
        return mock_df

    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(common.__dict__, 'postgres_schema', 'test_schema')
        mp.setitem(common.__dict__, 'read_query_from_postgres', mock_read_query)

        result = load_cntrt_col_assign(cntrt_id)
        pd.testing.assert_frame_equal(result, mock_df)

def test_load_cntrt_col_assign_empty_result():
    """Test with a contract ID that returns an empty DataFrame."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(common.__dict__, 'postgres_schema', 'test_schema')
        mp.setitem(common.__dict__, 'read_query_from_postgres', lambda q: pd.DataFrame())

        result = load_cntrt_col_assign(999)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

def test_load_cntrt_col_assign_with_none_id():
    """Test with None as contract ID."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(common.__dict__, 'postgres_schema', 'test_schema')
        mp.setitem(common.__dict__, 'read_query_from_postgres', lambda q: pd.DataFrame())

        result = load_cntrt_col_assign(None)
        assert isinstance(result, pd.DataFrame)

def test_load_cntrt_col_assign_with_string_id():
    """Test with a string as contract ID."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(common.__dict__, 'postgres_schema', 'test_schema')
        mp.setitem(common.__dict__, 'read_query_from_postgres', lambda q: pd.DataFrame())

        result = load_cntrt_col_assign("abc")
        assert isinstance(result, pd.DataFrame)
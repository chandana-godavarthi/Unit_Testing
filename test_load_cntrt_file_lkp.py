import pytest
import pandas as pd
import common  

@pytest.mark.parametrize("dmnsn_name", ["prod", "mkt", "fact"])
def test_load_cntrt_file_lkp_valid_dimensions(monkeypatch, dmnsn_name):
    expected_df = pd.DataFrame({"file_patrn": [f"{dmnsn_name}_pattern"]})

    def mock_read_query(query):
        assert f"dmnsn_name='{dmnsn_name}'" in query
        return expected_df

    monkeypatch.setitem(common.__dict__, "postgres_schema", "mock_schema")
    monkeypatch.setitem(common.__dict__, "read_query_from_postgres", mock_read_query)

    result = common.load_cntrt_file_lkp(101, dmnsn_name)
    pd.testing.assert_frame_equal(result, expected_df)

def test_load_cntrt_file_lkp_empty(monkeypatch):
    expected_df = pd.DataFrame()

    def mock_read_query(query):
        return expected_df

    monkeypatch.setitem(common.__dict__, "postgres_schema", "mock_schema")
    monkeypatch.setitem(common.__dict__, "read_query_from_postgres", mock_read_query)

    result = common.load_cntrt_file_lkp(999, "unknown")
    pd.testing.assert_frame_equal(result, expected_df)

def test_load_cntrt_file_lkp_exception(monkeypatch):
    def mock_read_query(query):
        raise Exception("Simulated DB failure")

    monkeypatch.setitem(common.__dict__, "postgres_schema", "mock_schema")
    monkeypatch.setitem(common.__dict__, "read_query_from_postgres", mock_read_query)

    with pytest.raises(Exception, match="Simulated DB failure"):
        common.load_cntrt_file_lkp(1, "prod")
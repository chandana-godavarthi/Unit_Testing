import pytest
import pandas as pd
import types
from common import load_cntrt_lkp
def test_load_cntrt_lkp():
    import common

    # Inject mock postgres_schema and read_query_from_postgres into the module
    common.postgres_schema = "mock_schema"

    mock_df = pd.DataFrame({'file_patrn': ['pattern1', 'pattern2']})
    cntrt_id = 123

    def mock_read_query(query):
        expected_query = f'''SELECT file_patrn FROM mock_schema.mm_cntrt_lkp WHERE cntrt_id= {cntrt_id}'''
        assert query == expected_query
        return mock_df

    common.read_query_from_postgres = mock_read_query

    # Now call the function
    result = common.load_cntrt_lkp(cntrt_id)

    # Validate the result
    pd.testing.assert_frame_equal(result, mock_df)
def test_load_cntrt_lkp_empty_result():
    import common
    common.postgres_schema = "mock_schema"
    common.read_query_from_postgres = lambda q: pd.DataFrame(columns=['file_patrn'])

    result = common.load_cntrt_lkp(123)
    assert result.empty
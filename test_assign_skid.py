import pytest
from unittest.mock import MagicMock, patch
from common import assign_skid

@pytest.fixture
def mock_df():
    df = MagicMock()
    df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']
    df.createOrReplaceTempView = MagicMock()
    df.select.return_value = df
    return df

@pytest.fixture
def mock_spark():
    spark = MagicMock()

    mock_sql_result = MagicMock()
    mock_sql_result.count.return_value = 0
    mock_sql_result.createOrReplaceTempView = MagicMock()
    mock_sql_result.select.return_value = mock_sql_result
    mock_sql_result.columns = ['run_id', 'extrn_prod_id', 'prod_skid']
    mock_sql_result.write.mode.return_value.saveAsTable = MagicMock()

    mock_final_result = MagicMock()
    mock_final_result.select.return_value = mock_final_result
    mock_final_result.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    def sql_side_effect(query):
        if "LIMIT 1" in query:
            return mock_sql_result
        elif "JOIN skid_df" in query:
            return mock_final_result
        else:
            return mock_sql_result

    spark.sql.side_effect = sql_side_effect
    return spark

def test_assign_skid_with_valid_prod_type(mock_df, mock_spark):
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(mock_df, 123, 'prod')
        assert result.columns == mock_df.columns

def test_assign_skid_with_valid_mkt_type(mock_df, mock_spark):
    mock_df.columns = ['run_id', 'extrn_mkt_id', 'mkt_skid']
    mock_sql_result = MagicMock()
    mock_sql_result.count.return_value = 0
    mock_sql_result.createOrReplaceTempView = MagicMock()
    mock_sql_result.select.return_value = mock_sql_result
    mock_sql_result.columns = ['run_id', 'extrn_mkt_id', 'mkt_skid']
    mock_sql_result.write.mode.return_value.saveAsTable = MagicMock()
    mock_spark.sql.side_effect = lambda query: mock_sql_result

    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(mock_df, 456, 'mkt')
        assert result.columns == mock_df.columns

def test_assign_skid_with_invalid_type_returns_none(mock_df):
    result = assign_skid(mock_df, 123, 'invalid')
    assert result is None

def test_assign_skid_skips_insert_if_run_id_exists(mock_df, mock_spark):
    def sql_side_effect(query):
        if "LIMIT 1" in query:
            mock_result = MagicMock()
            mock_result.count.return_value = 1
            return mock_result
        elif "JOIN skid_df" in query:
            mock_result = MagicMock()
            mock_result.select.return_value.columns = mock_df.columns
            return mock_result
        else:
            return MagicMock()
    mock_spark.sql.side_effect = sql_side_effect
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(mock_df, 789, 'prod')
        assert result.columns == mock_df.columns

def test_assign_skid_raises_exception_on_spark_error(mock_df):
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("Spark error")
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        with pytest.raises(Exception, match="Spark error"):
            assign_skid(mock_df, 123, 'prod')

def test_assign_skid_handles_empty_dataframe():
    df = MagicMock()
    df.columns = []
    df.createOrReplaceTempView = MagicMock()
    df.select.return_value = df
    mock_spark = MagicMock()
    mock_spark.sql.return_value = df
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(df, 123, 'prod')
        assert result.columns == []

def test_assign_skid_raises_exception_on_missing_column():
    df = MagicMock()
    df.columns = ['run_id']  # Missing extrn_prod_id
    df.createOrReplaceTempView = MagicMock()
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = Exception("Missing column")
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        with pytest.raises(Exception, match="Missing column"):
            assign_skid(df, 123, 'prod')

def test_assign_skid_accepts_case_insensitive_type(mock_df, mock_spark):
    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(mock_df, 123, 'PrOd')
        assert result.columns == mock_df.columns

def test_assign_skid_preserves_extra_columns(mock_df, mock_spark):
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid', 'extra_col']
    mock_sql_result = MagicMock()
    mock_sql_result.count.return_value = 0
    mock_sql_result.createOrReplaceTempView = MagicMock()
    mock_sql_result.select.return_value = mock_sql_result
    mock_sql_result.columns = ['run_id', 'extrn_prod_id', 'prod_skid', 'extra_col']
    mock_sql_result.write.mode.return_value.saveAsTable = MagicMock()
    mock_spark.sql.side_effect = lambda query: mock_sql_result

    with patch.dict(assign_skid.__globals__, {'spark': mock_spark, 'catalog_name': 'cdl_tp_dev'}):
        result = assign_skid(mock_df, 123, 'prod')
        assert 'extra_col' in result.columns
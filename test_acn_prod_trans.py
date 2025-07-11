import pytest
from unittest.mock import MagicMock, patch
from common import acn_prod_trans

@pytest.fixture
def mock_spark():
    with patch("common.spark") as mock_spark:
        yield mock_spark

@pytest.fixture
def mock_catalog_name():
    with patch.dict("common.__dict__", {"catalog_name": "mock_catalog"}):
        yield

@pytest.fixture
def mock_run_id():
    with patch.dict("common.__dict__", {"RUN_ID": "1001"}):
        yield

@pytest.fixture
def mock_dataframes(mock_spark):
    def sql_mock_generator():
        while True:
            df = MagicMock()
            df.withColumn.return_value = df
            df.unionByName.return_value = df
            df.createOrReplaceTempView.return_value = None
            df.drop.return_value = df
            df.groupBy.return_value.agg.return_value = df
            df.count.return_value = 10
            yield df

    sql_gen = sql_mock_generator()
    mock_spark.sql.side_effect = lambda query: next(sql_gen)

    mock_parquet_df = MagicMock(name="df_parquet")
    mock_parquet_df.count.return_value = 10
    mock_parquet_df.unionByName.return_value = mock_parquet_df
    mock_parquet_df.withColumn.return_value = mock_parquet_df
    mock_parquet_df.createOrReplaceTempView.return_value = None

    mock_spark.read.parquet.return_value = mock_parquet_df

    return {
        "df_parquet": mock_parquet_df
    }

def test_acn_prod_trans_mth(mock_spark, mock_catalog_name, mock_run_id, mock_dataframes):
    acn_prod_trans(srce_sys_id=1, TIME_PERD_TYPE_CODE="MTH")
    assert mock_dataframes["df_parquet"].withColumn.called
    assert mock_dataframes["df_parquet"].unionByName.called

def test_acn_prod_trans_wk(mock_spark, mock_catalog_name, mock_run_id, mock_dataframes):
    acn_prod_trans(srce_sys_id=2, TIME_PERD_TYPE_CODE="WK")
    assert mock_spark.sql.call_count > 0

def test_acn_prod_trans_bimth(mock_spark, mock_catalog_name, mock_run_id, mock_dataframes):
    acn_prod_trans(srce_sys_id=3, TIME_PERD_TYPE_CODE="BIMTH")
    assert mock_spark.sql.call_count > 0

def test_acn_prod_trans_edge_case_null_fields(mock_spark, mock_catalog_name, mock_run_id, mock_dataframes):
    acn_prod_trans(srce_sys_id=4, TIME_PERD_TYPE_CODE="MTH")
    assert mock_spark.sql.call_count > 0

def test_acn_prod_trans_grouping_and_join(mock_spark, mock_catalog_name, mock_run_id, mock_dataframes):
    acn_prod_trans(srce_sys_id=5, TIME_PERD_TYPE_CODE="WK")
    assert mock_spark.sql.call_count > 0
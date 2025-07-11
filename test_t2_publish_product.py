import pytest
from unittest.mock import MagicMock, patch
from common import t2_publish_product

@pytest.fixture
def mock_spark():
    with patch("common.spark") as mock_spark:
        yield mock_spark

def test_publish_product_sql_execution(mock_spark):
    df_mock = MagicMock()
    df_empty_mock = MagicMock()

    # Mock SQL return for limit 0
    mock_spark.sql.return_value = df_empty_mock
    df_mock.unionByName.return_value = df_mock

    t2_publish_product(df_mock, "test_catalog", "test_schema", "test_table")

    # Check SQL query for limit 0
    mock_spark.sql.assert_any_call("SELECT * FROM test_catalog.test_schema.test_table limit 0")

    # Check unionByName called with allowMissingColumns=True
    df_mock.unionByName.assert_called_once_with(df_empty_mock, allowMissingColumns=True)

    # Check temp view creation
    df_mock.createOrReplaceTempView.assert_called_once_with("df_mm_prod_sdim_promo_vw")

    # Check merge SQL execution
    expected_merge_sql = """
    MERGE INTO test_catalog.test_schema.test_table tgt
    USING df_mm_prod_sdim_promo_vw src
    ON src.srce_sys_id = tgt.srce_sys_id AND src.prod_skid = tgt.prod_skid
    WHEN MATCHED THEN
    UPDATE SET *
    WHEN NOT MATCHED THEN
    INSERT *
    """
    mock_spark.sql.assert_any_call(expected_merge_sql)

def test_publish_product_with_invalid_inputs(mock_spark):
    df_mock = MagicMock()
    mock_spark.sql.side_effect = Exception("Invalid SQL")

    with pytest.raises(Exception, match="Invalid SQL"):
        t2_publish_product(df_mock, "bad_catalog", "bad_schema", "bad_table")

def test_publish_product_with_empty_df(mock_spark):
    df_mock = MagicMock()
    df_empty_mock = MagicMock()

    mock_spark.sql.return_value = df_empty_mock
    df_mock.unionByName.return_value = df_mock

    t2_publish_product(df_mock, "catalog", "schema", "table")

    df_mock.unionByName.assert_called_once()
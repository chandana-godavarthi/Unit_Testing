import pytest
from unittest.mock import MagicMock
from common import acn_prod_trans_materialize

def test_acn_prod_trans_materialize_write_chain():
    df_mock = MagicMock()
    coalesced_df = MagicMock()
    writer = MagicMock()
    writer_options = MagicMock()
    writer_mode = MagicMock()

    # Chain mocks
    df_mock.coalesce.return_value = coalesced_df
    coalesced_df.write = writer
    writer.format.return_value = writer_options
    writer_options.options.return_value = writer_mode
    writer_mode.mode.return_value = writer_mode

    run_id = "20250710"
    expected_path = "/mnt/tp-publish-data/ACN_Prod_Load/20250710/ACN_Prod_Load_chain/tp_mm_ACN_Prod_Load_chain.parquet"

    acn_prod_trans_materialize(df_mock, run_id)

    df_mock.coalesce.assert_called_once_with(1)
    writer.format.assert_called_once_with("parquet")
    writer_options.options.assert_called_once_with(header=True)
    writer_mode.mode.assert_called_once_with("overwrite")
    writer_mode.save.assert_called_once_with(expected_path)

def test_acn_prod_trans_materialize_empty_run_id():
    df_mock = MagicMock()
    coalesced_df = MagicMock()
    writer = MagicMock()
    writer_options = MagicMock()
    writer_mode = MagicMock()

    df_mock.coalesce.return_value = coalesced_df
    coalesced_df.write = writer
    writer.format.return_value = writer_options
    writer_options.options.return_value = writer_mode
    writer_mode.mode.return_value = writer_mode

    run_id = ""
    expected_path = "/mnt/tp-publish-data/ACN_Prod_Load//ACN_Prod_Load_chain/tp_mm_ACN_Prod_Load_chain.parquet"

    acn_prod_trans_materialize(df_mock, run_id)

    writer_mode.save.assert_called_once_with(expected_path)

def test_acn_prod_trans_materialize_save_failure():
    df_mock = MagicMock()
    coalesced_df = MagicMock()
    writer = MagicMock()
    writer_options = MagicMock()
    writer_mode = MagicMock()

    df_mock.coalesce.return_value = coalesced_df
    coalesced_df.write = writer
    writer.format.return_value = writer_options
    writer_options.options.return_value = writer_mode
    writer_mode.mode.return_value = writer_mode
    writer_mode.save.side_effect = Exception("Write failed")

    run_id = "20250710"

    with pytest.raises(Exception, match="Write failed"):
        acn_prod_trans_materialize(df_mock, run_id)
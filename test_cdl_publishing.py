import pytest
from unittest.mock import MagicMock, patch
from common import cdl_publishing

@pytest.fixture
def mock_config_with_tables():
    return {
        "tables": ["table1", "table2"]
    }

@pytest.fixture
def mock_config_empty_tables():
    return {
        "tables": []
    }

@pytest.fixture
def mock_meta_client():
    client = MagicMock()
    client.mode.return_value = client
    client.publish_table = MagicMock()
    client.start_publishing = MagicMock()
    return client

def test_publish_tables_success(mock_config_with_tables, mock_meta_client):
    """Test publishing when config contains multiple tables."""
    mock_configuration = MagicMock()
    mock_configuration.load_for_default_environment_notebook.return_value = mock_config_with_tables
    mock_meta_ps_client = MagicMock()
    mock_meta_ps_client.configure.return_value.get_client.return_value = mock_meta_client
    mock_dbutils = MagicMock()

    with patch.dict(cdl_publishing.__globals__, {
        'Configuration': mock_configuration,
        'MetaPSClient': mock_meta_ps_client,
        'dbutils': mock_dbutils
    }):
        cdl_publishing("TP_WK_FCT", "TP_WK_FCT", "TP_WK_FCT", "partition_value")
        assert mock_meta_client.publish_table.call_count == len(mock_config_with_tables["tables"])
        mock_meta_client.start_publishing.assert_called_once()

def test_publish_with_empty_table_list(mock_config_empty_tables, mock_meta_client):
    """Test publishing when config contains no tables."""
    mock_configuration = MagicMock()
    mock_configuration.load_for_default_environment_notebook.return_value = mock_config_empty_tables
    mock_meta_ps_client = MagicMock()
    mock_meta_ps_client.configure.return_value.get_client.return_value = mock_meta_client
    mock_dbutils = MagicMock()

    with patch.dict(cdl_publishing.__globals__, {
        'Configuration': mock_configuration,
        'MetaPSClient': mock_meta_ps_client,
        'dbutils': mock_dbutils
    }):
        cdl_publishing("TP_WK_FCT", "TP_WK_FCT", "TP_WK_FCT", "partition_value")
        mock_meta_client.publish_table.assert_not_called()
        mock_meta_client.start_publishing.assert_called_once()

def test_publish_missing_tables_key_raises_keyerror(mock_meta_client):
    """Test publishing when config is missing the 'tables' key."""
    mock_config = {}
    mock_configuration = MagicMock()
    mock_configuration.load_for_default_environment_notebook.return_value = mock_config
    mock_meta_ps_client = MagicMock()
    mock_meta_ps_client.configure.return_value.get_client.return_value = mock_meta_client
    mock_dbutils = MagicMock()

    with patch.dict(cdl_publishing.__globals__, {
        'Configuration': mock_configuration,
        'MetaPSClient': mock_meta_ps_client,
        'dbutils': mock_dbutils
    }):
        with pytest.raises(KeyError):
            cdl_publishing("TP_WK_FCT", "TP_WK_FCT", "TP_WK_FCT", "partition_value")

def test_publish_table_raises_exception(mock_config_with_tables):
    """Test exception raised during publish_table call."""
    mock_meta_client = MagicMock()
    mock_meta_client.mode.return_value = mock_meta_client
    mock_meta_client.publish_table.side_effect = Exception("Publish failed")
    mock_meta_client.start_publishing = MagicMock()
    mock_configuration = MagicMock()
    mock_configuration.load_for_default_environment_notebook.return_value = mock_config_with_tables
    mock_meta_ps_client = MagicMock()
    mock_meta_ps_client.configure.return_value.get_client.return_value = mock_meta_client
    mock_dbutils = MagicMock()

    with patch.dict(cdl_publishing.__globals__, {
        'Configuration': mock_configuration,
        'MetaPSClient': mock_meta_ps_client,
        'dbutils': mock_dbutils
    }):
        with pytest.raises(Exception, match="Publish failed"):
            cdl_publishing("TP_WK_FCT", "TP_WK_FCT", "TP_WK_FCT", "partition_value")

def test_start_publishing_raises_exception(mock_config_with_tables, mock_meta_client):
    """Test exception raised during start_publishing call."""
    mock_meta_client.start_publishing.side_effect = Exception("Start failed")
    mock_configuration = MagicMock()
    mock_configuration.load_for_default_environment_notebook.return_value = mock_config_with_tables
    mock_meta_ps_client = MagicMock()
    mock_meta_ps_client.configure.return_value.get_client.return_value = mock_meta_client
    mock_dbutils = MagicMock()

    with patch.dict(cdl_publishing.__globals__, {
        'Configuration': mock_configuration,
        'MetaPSClient': mock_meta_ps_client,
        'dbutils': mock_dbutils
    }):
        with pytest.raises(Exception, match="Start failed"):
            cdl_publishing("TP_WK_FCT", "TP_WK_FCT", "TP_WK_FCT", "partition_value")
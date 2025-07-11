import pytest
from unittest.mock import patch, MagicMock
from common import get_dbutils

@pytest.fixture
def mock_spark():
    """Fixture to mock SparkSession."""
    return MagicMock(name="SparkSession")

def test_get_dbutils_with_pyspark_dbutils(mock_spark):
    """
    Test case: DBUtils is available via pyspark.dbutils.
    Expected: get_dbutils returns DBUtils instance.
    """
    mock_dbutils_instance = MagicMock(name="DBUtilsInstance")

    with patch.dict("sys.modules", {
        "pyspark.dbutils": MagicMock(DBUtils=MagicMock(return_value=mock_dbutils_instance))
    }):
        result = get_dbutils(mock_spark)
        assert result == mock_dbutils_instance

def test_get_dbutils_with_ipython_fallback(mock_spark):
    """
    Test case: DBUtils import fails, fallback to IPython with dbutils in user_ns.
    Expected: get_dbutils returns IPython dbutils instance.
    """
    mock_dbutils_instance = MagicMock(name="IPythonDBUtils")
    mock_user_ns = {"dbutils": mock_dbutils_instance}
    mock_ipython = MagicMock()
    mock_ipython.user_ns = mock_user_ns

    with patch.dict("sys.modules", {"pyspark.dbutils": None}), \
         patch("IPython.get_ipython", return_value=mock_ipython):
        result = get_dbutils(mock_spark)
        assert result == mock_dbutils_instance
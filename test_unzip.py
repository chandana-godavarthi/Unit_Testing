import pytest
from unittest.mock import patch, MagicMock
import common
from common import unzip

# Test: Successful unzip operation
def test_unzip_success():
    file_name = "sample.zip"
    catalog_name = "test_catalog"
    vol_path = "/mnt/test/"

    # Patch zipfile.ZipFile and its context manager behavior
    with patch("common.zipfile.ZipFile") as mock_zipfile:
        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        unzip(file_name, catalog_name, vol_path)

        mock_zipfile.assert_called_once_with(f"{vol_path}{file_name}", "r")
        mock_zip.extractall.assert_called_once_with(f"{vol_path}sample")

# Test: File name with mixed-case extension
def test_unzip_mixed_case_extension():
    file_name = "data.ZIP"
    catalog_name = "test_catalog"
    vol_path = "/mnt/test/"

    with patch("common.zipfile.ZipFile") as mock_zipfile:
        mock_zip = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        unzip(file_name, catalog_name, vol_path)

        mock_zip.extractall.assert_called_once_with(f"{vol_path}data")

# Test: Exception during unzip
def test_unzip_exception():
    file_name = "corrupt.zip"
    catalog_name = "test_catalog"
    vol_path = "/mnt/test/"

    with patch("common.zipfile.ZipFile", side_effect=Exception("Unzip failed")):
        with pytest.raises(Exception, match="Unzip failed"):
            unzip(file_name, catalog_name, vol_path)
import pytest
from unittest.mock import MagicMock
from common import work_to_arch

# Mock class to simulate dbutils.fs behavior
class MockDbutilsFS:
    def __init__(self, files):
        self.files = files
        self.mv_called_with = None

    def ls(self, path):
        return self.files

    def mv(self, src, dst):
        self.mv_called_with = (src, dst)

# Test when the file is found and moved successfully
def test_file_found_and_moved():
    file1 = MagicMock()
    file1.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip'
    file2 = MagicMock()
    file2.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_36073272025040603451400_998593513_20250406075153_1000000209_20250519_143329.zip'

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([file1, file2])

    result = work_to_arch("1000000384", "10000101753101", file1.name)
    assert result is True
    assert common.dbutils.fs.mv_called_with == (
        f'/mnt/tp-source-data/WORK/{file1.name}',
        f'/mnt/tp-source-data/ARCH/{file1.name}'
    )

# Test when the file is not found in the directory
def test_file_not_found():
    file1 = MagicMock()
    file1.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip'
    file2 = MagicMock()
    file2.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_36073272025040603451400_998593513_20250406075153_1000000209_20250519_143329.zip'

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([file1, file2])

    result = work_to_arch("1000000384", "10000101753101", "MAPWE.KR.POC.May25.DATA2025062514251200.ZIP_1000000382_20250704_032847.zip")
    assert result is False
    assert common.dbutils.fs.mv_called_with is None

# Test when the directory is empty
def test_empty_directory():
    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([])

    result = work_to_arch("1000000384", "10000101753101", "EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip")
    assert result is False

# Test when multiple files match the same name; only the first match is moved
def test_multiple_files_match_first():
    file1 = MagicMock()
    file1.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip'
    file2 = MagicMock()
    file2.name = file1.name  # Duplicate name

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([file1, file2])

    result = work_to_arch("1000000384", "10000101753101", file1.name)
    assert result is True
    assert common.dbutils.fs.mv_called_with == (
        f'/mnt/tp-source-data/WORK/{file1.name}',
        f'/mnt/tp-source-data/ARCH/{file1.name}'
    )

# Additional test case: Case sensitivity in file name matching
def test_case_sensitive_filename():
    file = MagicMock()
    file.name = 'EDWGE.ACN.CONNECT.US.REF.prod_1234_1000000384_20250707_230644.zip'  # Different case

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([file])

    result = work_to_arch("1000000384", "10000101753101", "EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip")
    assert result is False

# Additional test case: File name with leading/trailing whitespace
def test_filename_with_whitespace():
    file = MagicMock()
    file.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip'

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = MockDbutilsFS([file])

    result = work_to_arch("1000000384", "10000101753101", f" {file.name} ")
    assert result is False

# Additional test case: Exception during move operation
def test_mv_raises_exception():
    file = MagicMock()
    file.name = 'EDWGE.ACN.CONNECT.US.REF.PROD_1234_1000000384_20250707_230644.zip'

    class FailingDbutilsFS(MockDbutilsFS):
        def mv(self, src, dst):
            raise Exception("Move failed")

    import common
    common.dbutils = MagicMock()
    common.dbutils.fs = FailingDbutilsFS([file])

    try:
        result = work_to_arch("1000000384", "10000101753101", file.name)
        assert result is False  # If exception is caught internally
    except Exception as e:
        assert str(e) == "Move failed"
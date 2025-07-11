import pytest
from unittest.mock import MagicMock
import sys

# Patch dbutils before importing load_file
mock_dbutils = MagicMock()
sys.modules['dbutils'] = mock_dbutils

def setup_module(module):
    import common as lf
    lf.spark = MagicMock()
    lf.dbutils = mock_dbutils
    lf.display = MagicMock()
    lf.read_query_from_postgres = MagicMock()
    import os
    lf.os = os
    lf.os.path.join = lambda *a: '/'.join(a)
    lf.col = MagicMock(side_effect=lambda x: x)
    lf.F = MagicMock()
    lf.when = MagicMock()
    lf.concat_ws = MagicMock()

from common import load_file

# Helper to create a mock DataFrame with chained methods
def create_mock_df(columns):
    mock_df = MagicMock()
    mock_df.columns = columns
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.where.return_value = mock_df
    mock_df.distinct.return_value = mock_df
    mock_df.collect.return_value = []
    return mock_df

def test_load_file_fact():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = [{'file_col_name': 'A', 'db_col_name': 'B'}]
    mock_df = create_mock_df(['A'])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = [MagicMock(measr_phys_name='B')]
    result = load_file('fact', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_non_fact():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = [{'file_col_name': 'A', 'db_col_name': 'B'}]
    mock_df = create_mock_df(['A'])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = [MagicMock(measr_phys_name='B')]
    result = load_file('prod', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_handles_empty_columns():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = []
    mock_df = create_mock_df([])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    result = load_file('mkt', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_unsupported_file_type():
    import common as lf
    lf.dbutils.fs.ls.return_value = []
    mock_df = create_mock_df([])
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = []
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    result = load_file('unknown', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_no_zip_found():
    import common as lf
    lf.dbutils.fs.ls.return_value = [MagicMock(name='otherfile.csv', path='/mnt/tp-source-data/WORK/otherfile.csv')]
    mock_df = create_mock_df([])
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = []
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    result = load_file('prod', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_multiple_column_mappings():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = [{'file_col_name': 'A', 'db_col_name': 'B,C'}]
    mock_df = create_mock_df(['A'])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = [MagicMock(measr_phys_name='B'), MagicMock(measr_phys_name='C')]
    result = load_file('prod', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_special_char_column_mapping():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = [{'file_col_name': 'A', 'db_col_name': 'B#1'}]
    mock_df = create_mock_df(['A'])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = [MagicMock(measr_phys_name='B#1')]
    result = load_file('prod', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

def test_load_file_empty_input_file():
    import common as lf
    lf.spark.read.parquet.return_value.filter.return_value.where.return_value.select.return_value.distinct.return_value.collect.return_value = [{'file_col_name': 'A', 'db_col_name': 'B'}]
    mock_df = create_mock_df([])
    lf.spark.read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    lf.read_query_from_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    result = load_file('prod', 'RID', 'CID', 'PAT%', 'VEND', 'NB', ',')
    assert result == 'Success'

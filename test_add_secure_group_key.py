import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import common
from common import add_secure_group_key,spark

# Mock dbutils
class MockDbutils:
    class secrets:
        @staticmethod
        def get(scope, key):
            return "mock_schema"

# Mock query result
class MockQueryResult:
    def __init__(self, data):
        self.data = data
    def collect(self):
        return self.data

# Inject dbutils
common.dbutils = MockDbutils()

# Test: Contract-specific key found
def test_add_secure_group_key_found():
    def mock_query(query):
        if "mm_cntrt_secur_grp_key_assoc_vw" in query:
            return MockQueryResult([{"secure_group_key": 54300}])
        return MockQueryResult([{"secure_group_key": 54200}])
    common.read_query_from_postgres = mock_query

    df = spark.createDataFrame([("P&G",)], ["product"])
    result_df = common.add_secure_group_key(df, "C123")
    expected_df = df.withColumn("secure_group_key", lit(54300).cast(LongType()))
    assert result_df.collect() == expected_df.collect()

# Test: Contract-specific key missing, fallback to default
def test_add_secure_group_key_default():
    def mock_query(query):
        if "mm_cntrt_secur_grp_key_assoc_vw" in query:
            return MockQueryResult([{"secure_group_key": None}])
        return MockQueryResult([{"secure_group_key": 54200}])
    common.read_query_from_postgres = mock_query

    df = spark.createDataFrame([("P&G",)], ["product"])
    result_df = common.add_secure_group_key(df, "C999")
    expected_df = df.withColumn("secure_group_key", lit(54200).cast(LongType()))
    assert result_df.collect() == expected_df.collect()

# Test: Empty DataFrame
def test_add_secure_group_key_empty_df():
    def mock_query(query):
        if "mm_cntrt_secur_grp_key_assoc_vw" in query:
            return MockQueryResult([])
        return MockQueryResult([{"secure_group_key": 54200}])
    common.read_query_from_postgres = mock_query

    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame([], schema)
    result_df = common.add_secure_group_key(df, "C999")
    expected_df = df.withColumn("secure_group_key", lit(54200).cast(LongType()))
    assert result_df.collect() == expected_df.collect()

# Test: Multiple results, use first
def test_add_secure_group_key_multiple_results():
    def mock_query(query):
        if "mm_cntrt_secur_grp_key_assoc_vw" in query:
            return MockQueryResult([
                {"secure_group_key": 54300},
                {"secure_group_key": 60000}
            ])
        return MockQueryResult([{"secure_group_key": 54200}])
    common.read_query_from_postgres = mock_query

    df = spark.createDataFrame([("P&G",)], ["product"])
    result_df = common.add_secure_group_key(df, "C456")
    expected_df = df.withColumn("secure_group_key", lit(54300).cast(LongType()))
    assert result_df.collect() == expected_df.collect()

# Test: Contract ID is None
def test_add_secure_group_key_cntrt_id_none():
    def mock_query(query):
        if "mm_cntrt_secur_grp_key_assoc_vw" in query:
            return MockQueryResult([{"secure_group_key": None}])
        return MockQueryResult([{"secure_group_key": 54200}])
    common.read_query_from_postgres = mock_query

    df = spark.createDataFrame([("P&G",)], ["product"])
    result_df = common.add_secure_group_key(df, None)
    expected_df = df.withColumn("secure_group_key", lit(54200).cast(LongType()))
    assert result_df.collect() == expected_df.collect()
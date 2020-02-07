import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as pssf
import pyspark.sql.types as psst
import pytest
import sparklib

@pytest.fixture(scope='session')
def spark():
    os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"
    spark = SparkSession\
        .builder\
        .getOrCreate()
    yield spark
    spark.stop()

def test_sum(spark: SparkSession):
    df = spark.range(101)
    actual = sparklib.sum(df, 'id')
    expected = 5050

    assert actual == expected

def test_to_dict_should_sort(spark):
    input = spark.createDataFrame(
        [
            psst.Row(id=2, name='sam'),
            psst.Row(id=1, name='ivan')
        ]
    )
    expect = [
            { 'id': 1, 'name': 'ivan' },
            { 'id': 2, 'name': 'sam' },
        ]
    actual = sparklib.to_dict(input, ['id'])
    assert actual == expect

def test_find_duplicates(spark: SparkSession):
    input = spark.createDataFrame(
            [(1, 'a'),
             (2, 'b'),
             (1, 'c'),],
            ['id', 'value'])
    expect = spark.createDataFrame(
            [(1, 'a', None),
             (1, 'c', 'Duplicate key.'),
             (2, 'b', None),],                              
            ['id', 'value', 'error'])
    sort_by = ['id', 'value', 'error']

    actual = sparklib.tr_find_duplicates(input, "id")

    assert sparklib.to_dict(actual, sort_by) == sparklib.to_dict(expect, sort_by)


def test_as_list_deep(spark):
  input = [ psst.Row(id=1, a=psst.Row(b=101)) ]
  input_df = spark.createDataFrame(input)

  actual = sparklib.to_list(input_df, True)
  expect = [{'id':1, 'a':{'b':101}}]

  assert actual == expect

def test_as_list_shallow(spark):
  input = [ psst.Row(id=1, a=psst.Row(b=101)) ]
  input_df = spark.createDataFrame(input)

  actual = sparklib.to_list(input_df, False)
  expect = [{'id':1, 'a':psst.Row(b=101)}]

  assert actual == expect    

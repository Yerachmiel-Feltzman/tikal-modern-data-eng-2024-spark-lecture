from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from tikal_modern_data_eng_2024_spark_lecture.batch import read_cars_infer_schema, read_cars_with_schema


def test_read_cars_infer_schema(spark: SparkSession, data_sources: Path):
    df = read_cars_infer_schema(spark, data_sources / "cars.json")
    assert df.count() == 406


def test_read_cars_with_schema(spark: SparkSession, data_sources: Path):
    df = read_cars_with_schema(spark, data_sources / "cars.json")
    assert df.count() == 406


def test_read_cars_with_bad_schema(spark: SparkSession, data_sources: Path):
    # infer schema should work
    df = read_cars_infer_schema(spark, data_sources / "cars_bad_schema.json")
    assert df.schema.names.__contains__("foo")

    # but with fail fast schema enforcement should fail
    with pytest.raises(Py4JJavaError) as e:
        df = read_cars_with_schema(spark, data_sources / "cars_bad_schema.json")
        df.collect()

import datetime
from dataclasses import dataclass

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture
def sc(spark: SparkSession):
    return spark.sparkContext


@dataclass
class Person:
    name: str
    year_of_birth: int


def test_basic_rdd_example(sc: SparkContext):
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)
    print(rdd.collect())


def test_objects_rdd_example(sc: SparkContext):
    data = [
        Person("Bob", 1964),
        Person("Jack", 1990),
        Person("Marie", 1991),
        Person("Angela", 1986)
    ]
    rdd = sc.parallelize(data).cache()
    count = rdd.count()

    cumulative_age = (
        rdd
        .map(lambda p: datetime.date.today().year - p.year_of_birth)
        .reduce(lambda a1, a2: a1 + a2)
    )
    avg_age = cumulative_age / count

    assert sum([datetime.date.today().year - p.year_of_birth for p in data]) / len(data) == avg_age

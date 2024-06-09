import datetime
from dataclasses import dataclass
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, DataFrame, Row

from tikal_modern_data_eng_2024_spark_lecture.batch import read_cars_infer_schema, read_cars_with_schema, \
    avg_miles_per_gallon_per_year, extract_year_from_date, extract_brand_from_name, enrich_cars


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


@dataclass
class Car:
    Name: str = "foo"
    Year: datetime.date = datetime.date(1000, 1, 1)
    Miles_per_Gallon: int = 0
    Horsepower: int = 0


def test_extract_year_from_date(spark: SparkSession):
    cars = [
        Car(Year=datetime.date(1990, 1, 1)),
        Car(Year=datetime.date(1991, 1, 1)),
    ]
    cars = spark.createDataFrame(cars)

    df = extract_year_from_date(cars)
    assert df.select("Year").distinct().count() == 2


def test_cars_avg_miles_per_gallons(spark: SparkSession):
    cars = [
        Car(Year=datetime.date(1990, 1, 1), Miles_per_Gallon=10),
        Car(Year=datetime.date(1991, 1, 1), Miles_per_Gallon=20),
    ]
    cars = spark.createDataFrame(cars)

    df = avg_miles_per_gallon_per_year(cars)

    assert df.schema.names.__contains__("avg_miles_per_gallon")
    assert df.collect()[0] == Row(Year=1990, avg_miles_per_gallon=10)
    assert df.collect()[1] == Row(Year=1991, avg_miles_per_gallon=20)


def test_extract_brand_from_name(spark: SparkSession):
    cars = [
        Car("chevrolet chevelle malibu"),
        Car("buick skylark 320")
    ]
    cars = spark.createDataFrame(cars)
    cars_with_brand = extract_brand_from_name(cars)

    assert cars_with_brand.collect()[0].brand == "chevrolet"
    assert cars_with_brand.collect()[1].brand == "buick"


def test_transformations_chaining(spark: SparkSession):
    cars = [
        Car(Name="chevrolet chevelle malibu", Year=datetime.date(1990, 1, 1), Horsepower=10),
        Car(Name="buick skylark 320", Year=datetime.date(1992, 1, 1), Horsepower=20),
    ]
    cars = spark.createDataFrame(cars)

    df = enrich_cars(cars)
    assert df.collect()[0] == Row(brand="chevrolet", Year=1990, strongest_car_horsepower=10)
    assert df.collect()[1] == Row(brand="buick", Year=1992, strongest_car_horsepower=20)

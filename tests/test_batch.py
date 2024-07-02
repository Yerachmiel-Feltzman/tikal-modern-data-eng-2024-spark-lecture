import pytest
from chispa import assert_df_equality
from py4j.protocol import Py4JJavaError
from pyspark.sql import Row

from tests.models import *
from tikal_modern_data_eng_2024_spark_lecture.batch import *


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


def test_write_cars_naive(spark: SparkSession, data_output_folder):
    cars = [
        Car(Name="chevrolet chevelle malibu", Year=datetime.date(1990, 1, 1), Horsepower=10),
        Car(Name="buick skylark 320", Year=datetime.date(1992, 1, 1), Horsepower=20),
    ]
    cars = spark.createDataFrame(cars)

    cars_file_name = "cars.parquet"

    write_cars_naive(cars, path_prefix=data_output_folder, file_name=cars_file_name)

    result = spark.read.parquet((data_output_folder / cars_file_name).__str__())
    assert_df_equality(result, cars, ignore_column_order=True, ignore_row_order=True)


def test_write_cars_partition_naive(spark: SparkSession, data_output_folder: Path):
    cars = [
        Car(Name="chevrolet chevelle malibu", Year=datetime.date(1990, 1, 1), Horsepower=10),
        Car(Name="buick skylark 320", Year=datetime.date(1992, 1, 1), Horsepower=20),
    ]
    cars = spark.createDataFrame(cars)
    cars = enrich_cars(cars)

    cars_file_name = "cars.parquet"

    write_cars_partition_naive(cars,
                               path_prefix=data_output_folder,
                               file_name=cars_file_name,
                               partition_columns=["Year", "brand"])

    result = spark.read.parquet((data_output_folder / cars_file_name).__str__())

    assert_df_equality(result, cars, ignore_column_order=True, ignore_row_order=True)
    assert (data_output_folder / cars_file_name / "year=1990" / "brand=chevrolet").exists()


def test_write_transactions_partition(spark: SparkSession, data_output_folder: Path):
    transactions = [
        Transaction(date=datetime.date(2024, 10, 10), customer="David", price=150),
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=100),
    ]
    transactions = spark.createDataFrame(transactions)

    transactions_2 = [
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=150),
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=100),
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=200),
    ]
    transactions_2 = spark.createDataFrame(transactions_2)

    expected = [
        Transaction(date=datetime.date(2024, 10, 10), customer="David", price=150),

        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=150),
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=100),
        Transaction(date=datetime.date(2020, 10, 10), customer="David", price=200),
    ]
    expected = spark.createDataFrame(expected)

    transactions_file_name = "transactions.parquet"
    transactions.show()
    write_transactions_partition(transactions,
                                 path_prefix=data_output_folder,
                                 file_name=transactions_file_name,
                                 partition_columns=["date"])

    result = spark.read.parquet((data_output_folder / transactions_file_name).__str__())
    result.show()

    write_transactions_partition(transactions_2,
                                 path_prefix=data_output_folder,
                                 file_name=transactions_file_name,
                                 partition_columns=["date"])

    result = spark.read.parquet((data_output_folder / transactions_file_name).__str__())
    result.show()

    assert_df_equality(result, expected, ignore_column_order=True, ignore_row_order=True)


def test_enrich_transactions(spark: SparkSession):
    transactions = [
        Transaction(customer="David", price=150),
        Transaction(customer="Moses", price=100),
    ]
    transactions = spark.createDataFrame(transactions)

    expected = [
        Transaction(customer="David", price=150 * 4),
        Transaction(customer="Moses", price=100 * 4),
    ]
    expected = spark.createDataFrame(expected)

    result = enrich_transactions(transactions)

    assert result.collect()[0] == expected.collect()[0]
    assert result.collect()[1] == expected.collect()[1]


def test_collect_monitoring_metrics_for_transactions_pipeline(spark: SparkSession, data_sources: Path, capfd):
    transactions = spark.read.json((data_sources / "transactions.json").__str__())

    enriched = (
        transactions
        .withColumn("foo", F.lit(1))
        .withColumn("bar", F.lit("baz"))
        .where(transactions["price"] > 100)
    )

    collect_monitoring_metrics(enriched)
    enriched.explain()

    explains_output, err = capfd.readouterr()
    assert "InMemoryTableScan" in explains_output

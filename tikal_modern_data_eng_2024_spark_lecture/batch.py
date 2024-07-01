import logging
from multiprocessing import Array
from pathlib import Path
from typing import List

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, IntegerType

logger = logging.getLogger(__name__)


def read_cars_infer_schema(spark: SparkSession, path: Path) -> DataFrame:
    df = (
        spark.read
        .format("json")
        .option("path", path.__str__())
        .load()
    )

    return df


def read_cars_with_schema(spark: SparkSession, path: Path) -> DataFrame:
    schema = StructType([
        StructField("Name", StringType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Acceleration", DoubleType()),
        StructField("Year", DateType()),
        StructField("Origin", StringType())
    ])

    # docs: https://spark.apache.org/docs/latest/sql-data-sources-json.html
    df = (
        spark.read
        .format("json")
        .schema(schema)
        .option("mode", "FAILFAST")
        .option("path", path.__str__())
        .load()
    )

    return df


def extract_year_from_date(cars: DataFrame) -> DataFrame:
    # 1. fix year from date to year
    return cars.withColumn("Year", F.year(cars["Year"]))


def avg_miles_per_gallon_per_year(cars: DataFrame) -> DataFrame:
    # calculate avg miles per gallon per year
    df = (
        extract_year_from_date(cars)
        .groupby("Year")
        .agg(F.avg("Miles_per_Gallon").alias("avg_miles_per_gallon"))
    )

    return df


def extract_brand_from_name(cars: DataFrame) -> DataFrame:
    # example of UDF
    extract_brand_udf = F.udf(lambda name: name.split(" ")[0], StringType())

    return cars.withColumn("brand", extract_brand_udf("name"))


def enrich_cars(cars: DataFrame) -> DataFrame:
    # 1. fix year from date to year
    # 2. extract brand from name
    # 3. calculate avg horsepower per brand per year

    df = (
        cars
        .transform(extract_year_from_date)
        .transform(extract_brand_from_name)
        .groupby("brand", "Year")
        .agg(F.max("Horsepower").alias("strongest_car_horsepower"))
    )

    return df


def write_cars_naive(cars: DataFrame, path_prefix: Path, file_name: str = "cars") -> None:
    """
    https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#loading-data-programmatically
    """
    full_path = path_prefix / file_name
    (
        cars
        .write
        .parquet(full_path.__str__())
    )


def write_cars_partition_naive(cars: DataFrame,
                               path_prefix: Path,
                               file_name: str = "cars",
                               partition_columns: List[str] = None) -> None:
    full_path = path_prefix / file_name
    (
        cars
        .write
        .partitionBy(partition_columns)
        .parquet(full_path.__str__())
    )


def write_transactions_partition(transactions: DataFrame,
                                 path_prefix: Path,
                                 file_name: str = "transactions",
                                 partition_columns: List[str] = None) -> None:
    full_path = path_prefix / file_name

    # 1: first without overwrite -> will fail
    # 2: without dynamic partition -> will overwrite partitions we don't want (like 2024)
    # 3: will work

    (
        transactions
        .write
        .partitionBy(partition_columns)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(full_path.__str__())
    )


def read(spark: SparkSession, path: Path) -> DataFrame:
    transactions = spark.read.json(path.__str__())
    return transactions


def enrich_transactions(transactions: DataFrame) -> DataFrame:
    @pandas_udf(returnType=IntegerType())
    def convert_to_nis(usd: pd.Series) -> pd.Series:
        return usd * 4

    return transactions.withColumn("price", convert_to_nis("price"))


def collect_monitoring_metrics(transactions: DataFrame) -> None:
    transactions.cache()

    count = transactions.count()

    late_arrives = (
        transactions
        .where(transactions["date"] <= F.date_sub(F.current_date(), 1))
        .count()
    )

    # send
    print("------- METRICS SENT TO MONITORING SYSTEM -------")
    print(f"count: {count}")
    print(f"late_arrives: {late_arrives}")
    print("dataframe: ")
    transactions.show()
    print("-------------------------------------------------")


def write(transactions: DataFrame, path: Path):
    path.mkdir(exist_ok=True)

    write_transactions_partition(transactions,
                                 path_prefix=path,
                                 partition_columns=["date"])


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    transactions = read(spark, Path("../data/sources/transactions.json"))

    enriched = enrich_transactions(transactions)

    collect_monitoring_metrics(enriched)

    write(enriched, path=Path("../data/output/"))


if __name__ == '__main__':
    main()

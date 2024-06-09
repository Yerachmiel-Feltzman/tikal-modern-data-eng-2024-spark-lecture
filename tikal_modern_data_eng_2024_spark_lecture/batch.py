import logging
from multiprocessing import Array
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

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


def main():
    # sources = Path("data/sources/")
    #
    # spark = SparkSession.builder.master("local[*]").getOrCreate()
    # df = read_cars_with_schema(spark, sources / "cars.json")
    # df.printSchema()
    # df.show()

    pass


if __name__ == '__main__':
    main()

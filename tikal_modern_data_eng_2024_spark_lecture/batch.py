import logging
from multiprocessing import Array
from pathlib import Path

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


def main():
    sources = Path("data/sources/")

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    df = read_cars_with_schema(spark, sources / "cars.json")
    df.printSchema()
    df.show()


if __name__ == '__main__':
    main()

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main():
    builder = (
        SparkSession
        .builder
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    data = spark.range(0, 5)
    data.write.mode("overwrite").format("delta").save("/tmp/delta-table")
    df = spark.read.format("delta").load("/tmp/delta-table")
    df.show()


if __name__ == '__main__':
    main()

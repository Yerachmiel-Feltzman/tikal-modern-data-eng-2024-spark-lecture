from pyspark.sql import SparkSession


def test_setup(spark: SparkSession):
    dummy_df = [('I', "am", "running", "just", "fine")]
    spark.createDataFrame(dummy_df).show()

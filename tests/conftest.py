import uuid
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession
        .builder
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
    # return SparkSession \
    #     .builder \
    #     .master("local[*]") \
    #     .getOrCreate()


@pytest.fixture()
def data_sources():
    return Path("../data/sources/")


@pytest.fixture(scope="function")
def data_output_folder():
    out = Path("../data/output/") / uuid.uuid4().__str__()
    out.mkdir(exist_ok=True, parents=True)
    return out

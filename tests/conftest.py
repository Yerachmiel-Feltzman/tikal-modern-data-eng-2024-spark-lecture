from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.master("local[*]").getOrCreate()


@pytest.fixture()
def data_sources():
    return Path("../data/sources/")


@pytest.fixture()
def data_output_folder():
    out = Path("../data/output/")
    out.mkdir(exist_ok=True)
    return out

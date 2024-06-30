import logging
from multiprocessing import Array
from pathlib import Path
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType

logger = logging.getLogger(__name__)


def stream_from_socket_to_console(spark) -> StreamingQuery:
    # nc -lk 9999
    source_df = (
        spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    result_df = source_df
    # {"customer":"David","date":"2020-10-10","price":200}
    # op: "u|i|d
    # fullDoc
    # partialDoc

    query = (
        result_df
        .writeStream
        .format("console")
        .start()
    )

    return query


def parse_transactions_cdc(cdc: DataFrame) -> DataFrame:
    dedup_prep = cdc.withColumn(
        "rank",
        F.rank().over(Window.partitionBy("pk").orderBy(F.desc("offset")))
    )

    dedup_prep.show()

    result = (
        dedup_prep
        .where(dedup_prep.rank == 1)
        .withColumn("is_deleted", cdc.op == "d")
        .select(
            "fullDoc.customer",
            "fullDoc.date",
            "fullDoc.price",
            "is_deleted",
            "pk"
        )
    )

    return result


def read(spark: SparkSession, path: Path) -> DataFrame:
    source_df = (
        spark
        .readStream
        .format("socket")
        .option("host")
        .option("port", 9999)
        .load()
    )

    return source_df


def enrich(spark, source_df: DataFrame) -> DataFrame:
    return source_df


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # transactions = read(spark, Path("../data/sources/transactions.json"))
    #
    # enriched = enrich_transactions(transactions)
    #
    # collect_monitoring_metrics(enriched)

    # write(enriched, path=Path("../data/output/"))

    streaming_query = stream_from_socket_to_console(spark)

    streaming_query.awaitTermination()


if __name__ == '__main__':
    main()

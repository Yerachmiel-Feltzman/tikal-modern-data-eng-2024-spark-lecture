import logging
from multiprocessing import Array
from pathlib import Path
from typing import List

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, IntegerType, \
    BooleanType

from tikal_modern_data_eng_2024_spark_lecture import batch

logger = logging.getLogger(__name__)


def read_from_socket(spark: SparkSession) -> DataFrame:
    # nc -lk 9999
    source_df = (
        spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    return source_df


def write_to_console(result_df) -> StreamingQuery:
    query = (
        result_df
        .writeStream
        .format("console")
        .start()
    )

    return query


def stream_from_socket_to_console(spark: SparkSession) -> StreamingQuery:
    source = read_from_socket(spark)
    return write_to_console(source)


def parse_transactions_cdc(cdc: DataFrame) -> DataFrame:
    # if cdc.isEmpty():
    #     return cdc

    # {"customer":"David","date":"2020-10-10","price":200}
    # op: "u|i|d
    # fullDoc

    dedup_prep = cdc.withColumn(
        "rank",
        F.rank().over(Window.partitionBy("pk").orderBy(F.desc("offset")))
    )

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


def stream_transactions_cdc(spark: SparkSession) -> StreamingQuery:
    cdc = read_transactions_cdc_from_socket(spark)

    def batch_processing(df, epoch_id):
        parsed_cdc = parse_transactions_cdc(df)

        enriched = batch.enrich_transactions(parsed_cdc)

        batch.collect_monitoring_metrics(enriched)

        batch.write(enriched, path=Path("../data/output/"))

    query = (
        cdc
        .writeStream
        .foreachBatch(batch_processing)
        .start()
    )

    return query


def read_transactions_cdc_from_socket(spark):
    socket = read_from_socket(spark)
    schema = StructType([
        StructField("pk", IntegerType()),
        StructField("op", StringType()),
        StructField("offset", IntegerType()),
        StructField("fullDoc", StructType([
            StructField("customer", StringType()),
            StructField("price", IntegerType()),
            StructField("date", DateType()),
            StructField("is_deleted", BooleanType()),
            StructField("pk", IntegerType()),

        ])),
    ])
    cdc = socket.withColumn("value", F.from_json(F.cast("str", "value"), schema)).select("value.*")
    return cdc


def main():
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # transactions = read(spark, Path("../data/sources/transactions.json"))
    #
    # enriched = enrich_transactions(transactions)
    #
    # collect_monitoring_metrics(enriched)

    # write(enriched, path=Path("../data/output/"))

    # streaming_query = stream_from_socket_to_console(spark)
    streaming_query = stream_transactions_cdc(spark)

    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
